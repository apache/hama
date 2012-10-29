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
package org.apache.hama.examples;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.Combiner;
import org.apache.hama.bsp.HashPartitioner;
import org.apache.hama.bsp.TextInputFormat;
import org.apache.hama.bsp.TextOutputFormat;
import org.apache.hama.graph.Edge;
import org.apache.hama.graph.GraphJob;
import org.apache.hama.graph.Vertex;
import org.apache.hama.graph.VertexInputReader;

/**
 * Finding the mindist vertex in a connected component.
 */
public class MindistSearch {

  /*
   * Make sure that you know that you're comparing text, and not integers!
   */
  public static class MindistSearchVertex extends
      Vertex<Text, NullWritable, Text> {

    @Override
    public void compute(Iterator<Text> messages) throws IOException {
      Text currentComponent = getValue();
      if (getSuperstepCount() == 0L) {
        // if we have no associated component, pick the lowest in our direct
        // neighbourhood.
        if (currentComponent == null) {
          setValue(new Text(getVertexID()));
          for (Edge<Text, NullWritable> e : getEdges()) {
            Text id = getVertexID();
            if (id.compareTo(e.getDestinationVertexID()) > 0) {
              setValue(e.getDestinationVertexID());
            }
          }
          sendMessageToNeighbors(getValue());
        }
      } else {
        boolean updated = false;
        while (messages.hasNext()) {
          Text next = messages.next();
          if (currentComponent != null && next != null) {
            if (currentComponent.compareTo(next) > 0) {
              updated = true;
              setValue(next);
            }
          }
        }
        if (updated) {
          sendMessageToNeighbors(getValue());
        } else {
          this.voteToHalt();
        }
      }
    }
  }

  public static class MinTextCombiner extends Combiner<Text> {

    @Override
    public Text combine(Iterable<Text> messages) {
      Text min = null;
      for (Text m : messages) {
        if (min == null || min.compareTo(m) > 0) {
          min = m;
        }
      }
      return min;
    }

  }

  public static class MindistSearchCountReader extends
      VertexInputReader<LongWritable, Text, Text, NullWritable, Text> {

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
        Vertex<Text, NullWritable, Text> vertex) throws Exception {
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

  private static void printUsage() {
    System.out
        .println("Usage: <input> <output> [maximum iterations (default 30)] [tasks]");
    System.exit(-1);
  }

  public static void main(String[] args) throws IOException,
      InterruptedException, ClassNotFoundException {
    if (args.length < 2)
      printUsage();

    HamaConfiguration conf = new HamaConfiguration(new Configuration());
    GraphJob job = new GraphJob(conf, MindistSearchVertex.class);
    job.setJobName("Mindist Search");

    job.setVertexClass(MindistSearchVertex.class);
    job.setInputPath(new Path(args[0]));
    job.setOutputPath(new Path(args[1]));
    // set the min text combiner here
    job.setCombinerClass(MinTextCombiner.class);

    // set the defaults
    job.setMaxIteration(30);
    if (args.length == 4)
      job.setNumBspTask(Integer.parseInt(args[3]));
    if (args.length >= 3)
      job.setMaxIteration(Integer.parseInt(args[2]));

    job.setVertexIDClass(Text.class);
    job.setVertexValueClass(Text.class);
    job.setEdgeValueClass(NullWritable.class);

    job.setInputKeyClass(LongWritable.class);
    job.setInputValueClass(Text.class);
    job.setInputFormat(TextInputFormat.class);
    job.setVertexInputReaderClass(MindistSearchCountReader.class);
    job.setPartitioner(HashPartitioner.class);
    job.setOutputFormat(TextOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    long startTime = System.currentTimeMillis();
    if (job.waitForCompletion(true)) {
      System.out.println("Job Finished in "
          + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
    }
  }

}
