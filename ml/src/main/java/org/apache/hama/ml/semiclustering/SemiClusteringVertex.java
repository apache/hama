/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hama.ml.semiclustering;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.graph.Edge;
import org.apache.hama.graph.Vertex;

/**
 * SemiClusteringVertex Class defines each vertex in a Graph job and the
 * compute() method is the function which is applied on each Vertex in the graph
 * on each Super step of the job execution.
 * 
 */
public class SemiClusteringVertex extends
    Vertex<Text, DoubleWritable, SemiClusterMessage> {
  private static int semiClusterMaximumVertexCount;
  private static int graphJobMessageSentCount;
  private static int graphJobVertexMaxClusterCount;

  @Override
  public void setup(HamaConfiguration conf) {
    semiClusterMaximumVertexCount = conf.getInt("semicluster.max.vertex.count", 10);
    graphJobMessageSentCount = conf.getInt("semicluster.max.message.sent.count", 10);
    graphJobVertexMaxClusterCount = conf.getInt("vertex.max.cluster.count", 10);
  }

  /**
   * The user overrides the Compute() method, which will be executed at each
   * active vertex in every superstep
   */
  @Override
  public void compute(Iterable<SemiClusterMessage> messages) throws IOException {
    if (this.getSuperstepCount() == 0) {
      initClusters();
    }

    if (this.getSuperstepCount() >= 1) {
      TreeSet<SemiClusterMessage> candidates = new TreeSet<SemiClusterMessage>();

      for (SemiClusterMessage msg : messages) {
        candidates.add(msg);

        if (!msg.contains(this.getVertexID())
            && msg.size() == semiClusterMaximumVertexCount) {
          SemiClusterMessage msgNew = WritableUtils.clone(msg, this.getConf());
          msgNew.addVertex(this);
          msgNew.setSemiClusterId("C"
              + createNewSemiClusterName(msgNew.getVertexList()));
          msgNew.setScore(semiClusterScoreCalcuation(msgNew));

          candidates.add(msgNew);
        }
      }

      Iterator<SemiClusterMessage> bestCandidates = candidates.descendingIterator();
      int count = 0;

      while (bestCandidates.hasNext() && count < graphJobMessageSentCount) {
        SemiClusterMessage candidate = bestCandidates.next();
        sendMessageToNeighbors(candidate);
        count++;
      }

      // Update candidates
      SemiClusterMessage value = this.getValue();
      Set<SemiClusterDetails> clusters = value.getSemiClusterContainThis();
      for (SemiClusterMessage msg : candidates) {
        if (clusters.size() > graphJobVertexMaxClusterCount) {
          break;
        } else {
          clusters.add(new SemiClusterDetails(msg.getSemiClusterId(), msg.getScore()));
        }
      }

      this.setValue(value);
    }
  }

  private void initClusters() throws IOException {
    List<Vertex<Text, DoubleWritable, SemiClusterMessage>> lV = new ArrayList<Vertex<Text, DoubleWritable, SemiClusterMessage>>();
    lV.add(WritableUtils.clone(this, this.getConf()));
    String newClusterName = "C" + createNewSemiClusterName(lV);
    SemiClusterMessage initialClusters = new SemiClusterMessage();
    initialClusters.setSemiClusterId(newClusterName);
    initialClusters.addVertexList(lV);
    initialClusters.setScore(1);

    sendMessageToNeighbors(initialClusters);

    Set<SemiClusterDetails> scList = new TreeSet<SemiClusterDetails>();
    scList.add(new SemiClusterDetails(newClusterName, 1.0));
    SemiClusterMessage vertexValue = new SemiClusterMessage();
    vertexValue.setSemiClusterContainThis(scList);
    this.setValue(vertexValue);
  }

  /**
   * This function create a new Semi-cluster ID for a semi-cluster from the list
   * of vertices in the cluster.It first take all the vertexIds as a list sort
   * the list and then find the HashCode of the Sorted List.
   */
  public int createNewSemiClusterName(
      List<Vertex<Text, DoubleWritable, SemiClusterMessage>> semiClusterVertexList) {
    List<String> vertexIDList = new ArrayList<String>();
    for (Vertex<Text, DoubleWritable, SemiClusterMessage> v : semiClusterVertexList) {
      vertexIDList.add(v.getVertexID().toString());
    }
    Collections.sort(vertexIDList);
    return (vertexIDList.hashCode());
  }

  /**
   * Function to calcualte the Score of a semi-cluster
   * 
   * @param message
   * @return
   */
  public double semiClusterScoreCalcuation(SemiClusterMessage message) {
    double iC = 0.0, bC = 0.0, fB = 0.0, sC = 0.0;
    int vC = 0, eC = 0;
    vC = message.size();
    for (Vertex<Text, DoubleWritable, SemiClusterMessage> v : message
        .getVertexList()) {
      List<Edge<Text, DoubleWritable>> eL = v.getEdges();
      for (Edge<Text, DoubleWritable> e : eL) {
        eC++;
        if (message.contains(e.getDestinationVertexID())
            && e.getValue() != null) {
          iC = iC + e.getValue().get();
        } else if (e.getValue() != null) {
          bC = bC + e.getValue().get();
        }
      }
    }
    if (vC > 1)
      sC = ((iC - fB * bC) / ((vC * (vC - 1)) / 2)) / eC;
    return sC;
  }

}
