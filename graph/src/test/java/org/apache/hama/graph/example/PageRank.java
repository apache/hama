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
package org.apache.hama.graph.example;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.graph.AbstractAggregator;
import org.apache.hama.graph.Edge;
import org.apache.hama.graph.Vertex;
import org.apache.hama.graph.VertexInputReader;

public class PageRank {

  public static class PageRankVertex extends
      Vertex<Text, NullWritable, DoubleWritable> {

    static double DAMPING_FACTOR = 0.85;
    static double MAXIMUM_CONVERGENCE_ERROR = 0.001;

    int numEdges;

    @Override
    public void setup(Configuration conf) {
      String val = conf.get("hama.pagerank.alpha");
      if (val != null) {
        DAMPING_FACTOR = Double.parseDouble(val);
      }
      val = conf.get("hama.graph.max.convergence.error");
      if (val != null) {
        MAXIMUM_CONVERGENCE_ERROR = Double.parseDouble(val);
      }
      numEdges = this.getEdges().size();
    }

    @Override
    public void compute(Iterator<DoubleWritable> messages) throws IOException {
      // initialize this vertex to 1 / count of global vertices in this graph
      if (this.getSuperstepCount() == 0) {
        this.setValue(new DoubleWritable(1.0 / this.getNumVertices()));
      } else if (this.getSuperstepCount() >= 1) {
        DoubleWritable danglingNodeContribution = getLastAggregatedValue(1);
        double sum = 0;
        while (messages.hasNext()) {
          DoubleWritable msg = messages.next();
          sum += msg.get();
        }
        if (danglingNodeContribution == null) {
          double alpha = (1.0d - DAMPING_FACTOR) / this.getNumVertices();
          this.setValue(new DoubleWritable(alpha + (DAMPING_FACTOR * sum)));
        } else {
          double alpha = (1.0d - DAMPING_FACTOR) / this.getNumVertices();
          this.setValue(new DoubleWritable(alpha
              + (DAMPING_FACTOR * (sum + danglingNodeContribution.get()
                  / this.getNumVertices()))));
        }
      }

      // if we have not reached our global error yet, then proceed.
      DoubleWritable globalError = getLastAggregatedValue(0);
      if (globalError != null && this.getSuperstepCount() > 2
          && MAXIMUM_CONVERGENCE_ERROR > globalError.get()) {
        voteToHalt();
        return;
      }
      // in each superstep we are going to send a new rank to our neighbours
      sendMessageToNeighbors(new DoubleWritable(this.getValue().get()
          / numEdges));
    }
  }

  public static class PagerankTextReader extends
      VertexInputReader<LongWritable, Text, Text, NullWritable, DoubleWritable> {

    /**
     * The text file essentially should look like: <br/>
     * VERTEX_ID\t(n-tab separated VERTEX_IDs)<br/>
     * E.G:<br/>
     * 1\t2\t3\t4<br/>
     * 2\t3\t1<br/>
     * etc.
     */
    @Override
    public boolean parseVertex(LongWritable key, Text value,
        Vertex<Text, NullWritable, DoubleWritable> vertex) {
      String[] split = value.toString().split("\t");
      for (int i = 0; i < split.length; i++) {
        if (i == 0) {
          vertex.setVertexID(new Text(split[i]));
        } else {
          vertex
              .addEdge(new Edge<Text, NullWritable>(new Text(split[i]), null));
        }
      }
      return true;
    }

  }

  public static class DanglingNodeAggregator
      extends
      AbstractAggregator<DoubleWritable, Vertex<Text, NullWritable, DoubleWritable>> {

    double danglingNodeSum;

    @Override
    public void aggregate(Vertex<Text, NullWritable, DoubleWritable> vertex,
        DoubleWritable value) {
      if (vertex != null) {
        if (vertex.getEdges().size() == 0) {
          danglingNodeSum += value.get();
        }
      } else {
        danglingNodeSum += value.get();
      }
    }

    @Override
    public DoubleWritable getValue() {
      return new DoubleWritable(danglingNodeSum);
    }

  }

}
