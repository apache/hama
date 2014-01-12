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
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
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
    semiClusterMaximumVertexCount = conf.getInt("semicluster.max.vertex.count",
        10);
    graphJobMessageSentCount = conf.getInt(
        "semicluster.max.message.sent.count", 10);
    graphJobVertexMaxClusterCount = conf.getInt("vertex.max.cluster.count", 10);
  }

  /**
   * The user overrides the Compute() method, which will be executed at each
   * active vertex in every superstep
   */
  @Override
  public void compute(Iterable<SemiClusterMessage> messages) throws IOException {
    if (this.getSuperstepCount() == 0) {
      firstSuperStep();
    }

    if (this.getSuperstepCount() >= 1) {
      Set<SemiClusterMessage> scListContainThis = new TreeSet<SemiClusterMessage>();
      Set<SemiClusterMessage> scListNotContainThis = new TreeSet<SemiClusterMessage>();
      List<SemiClusterMessage> scList = new ArrayList<SemiClusterMessage>();
      for (SemiClusterMessage msg : messages) {
        if (!isVertexInSc(msg)) {
          scListNotContainThis.add(msg);
          SemiClusterMessage msgNew = new SemiClusterMessage(msg);
          msgNew.addVertex(this);
          msgNew
              .setScId("C" + createNewSemiClusterName(msgNew.getVertexList()));
          msgNew.setScore(semiClusterScoreCalcuation(msgNew));
          scListContainThis.add(msgNew);
        } else {
          scListContainThis.add(msg);
        }
      }
      scList.addAll(scListContainThis);
      scList.addAll(scListNotContainThis);
      sendBestSCMsg(scList);
      updatesVertexSemiClustersList(scListContainThis);
    }
  }

  public List<SemiClusterMessage> addSCList(List<SemiClusterMessage> scList,
      SemiClusterMessage msg) {
    return scList;
  }

  /**
   * This function create a new Semi-cluster ID for a semi-cluster from the list
   * of vertices in the cluster.It first take all the vertexIds as a list sort
   * the list and then find the HashCode of the Sorted List.
   */
  public int createNewSemiClusterName(
      List<Vertex<Text, DoubleWritable, SemiClusterMessage>> semiClusterVertexList) {
    List<String> vertexIDList = getSemiClusterVerticesIdList(semiClusterVertexList);
    Collections.sort(vertexIDList);
    return (vertexIDList.hashCode());
  }

  /**
   * Function which is executed in the first SuperStep
   * 
   * @throws java.io.IOException
   */
  public void firstSuperStep() throws IOException {
    Vertex<Text, DoubleWritable, SemiClusterMessage> v = this.deepCopy();
    List<Vertex<Text, DoubleWritable, SemiClusterMessage>> lV = new ArrayList<Vertex<Text, DoubleWritable, SemiClusterMessage>>();
    lV.add(v);
    String newClusterName = "C" + createNewSemiClusterName(lV);
    SemiClusterMessage initialClusters = new SemiClusterMessage(newClusterName,
        lV, 1);
    sendMessageToNeighbors(initialClusters);

    Set<SemiClusterDetails> scList = new TreeSet<SemiClusterDetails>();
    scList.add(new SemiClusterDetails(newClusterName, 1.0));
    SemiClusterMessage vertexValue = new SemiClusterMessage(scList);
    this.setValue(vertexValue);
  }

  /**
   * Vertex V updates its list of semi-clusters with the semi- clusters from c1
   * , ..., ck , c'1 , ..., c'k that contain V
   */
  public void updatesVertexSemiClustersList(
      Set<SemiClusterMessage> scListContainThis) throws IOException {
    List<SemiClusterDetails> scList = new ArrayList<SemiClusterDetails>();
    Set<SemiClusterMessage> sortedSet = new TreeSet<SemiClusterMessage>(
        new Comparator<SemiClusterMessage>() {

          @Override
          public int compare(SemiClusterMessage o1, SemiClusterMessage o2) {
            return (o1.getScore() == o2.getScore() ? 0
                : o1.getScore() < o2.getScore() ? -1 : 1);
          }
        });
    sortedSet.addAll(scListContainThis);
    int count = 0;
    for (SemiClusterMessage msg : sortedSet) {
      scList.add(new SemiClusterDetails(msg.getScId(), msg.getScore()));
      if (count > graphJobMessageSentCount)
        break;
    }

    SemiClusterMessage vertexValue = this.getValue();
    vertexValue
        .setSemiClusterContainThis(scList, graphJobVertexMaxClusterCount);
    this.setValue(vertexValue);
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
    List<String> vertexId = getSemiClusterVerticesIdList(message
        .getVertexList());
    vC = vertexId.size();
    for (Vertex<Text, DoubleWritable, SemiClusterMessage> v : message
        .getVertexList()) {
      List<Edge<Text, DoubleWritable>> eL = v.getEdges();
      for (Edge<Text, DoubleWritable> e : eL) {
        eC++;
        if (vertexId.contains(e.getDestinationVertexID().toString())
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

  /**
   * Returns a Array List of vertexIds from a List of Vertex<Text,
   * DoubleWritable, SCMessage> Objects
   * 
   * @param lV
   * @return
   */
  public List<String> getSemiClusterVerticesIdList(
      List<Vertex<Text, DoubleWritable, SemiClusterMessage>> lV) {
    Iterator<Vertex<Text, DoubleWritable, SemiClusterMessage>> vertexItrator = lV
        .iterator();
    List<String> vertexId = new ArrayList<String>();
    while (vertexItrator.hasNext()) {
      vertexId.add(vertexItrator.next().getVertexID().toString());
    }

    return vertexId;
  }

  /**
   * If a semi-cluster c does not already contain V , and Vc < Mmax , then V is
   * added to c to form c' .
   */
  public boolean isVertexInSc(SemiClusterMessage msg) {
    List<String> vertexId = getSemiClusterVerticesIdList(msg.getVertexList());
    return vertexId.contains(this.getVertexID().toString())
        && vertexId.size() < semiClusterMaximumVertexCount;
  }

  /**
   * The semi-clusters c1 , ..., ck , c'1 , ..., c'k are sorted by their scores,
   * and the best ones are sent to V ?? neighbors.
   */
  public void sendBestSCMsg(List<SemiClusterMessage> scList) throws IOException {
    Collections.sort(scList, new Comparator<SemiClusterMessage>() {

      @Override
      public int compare(SemiClusterMessage o1, SemiClusterMessage o2) {
        return (o1.getScore() == o2.getScore() ? 0 : o1.getScore() < o2
            .getScore() ? -1 : 1);
      }
    });
    Iterator<SemiClusterMessage> scItr = scList.iterator();
    int count = 0;
    while (scItr.hasNext()) {
      sendMessageToNeighbors(scItr.next());
      count++;
      if (count > graphJobMessageSentCount)
        break;
    }
  }
}
