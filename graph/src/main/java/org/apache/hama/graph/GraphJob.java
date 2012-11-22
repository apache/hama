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
package org.apache.hama.graph;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.Combiner;
import org.apache.hama.bsp.HashPartitioner;
import org.apache.hama.bsp.Partitioner;

import com.google.common.base.Preconditions;

public class GraphJob extends BSPJob {

  public final static String VERTEX_CLASS_ATTR = "hama.graph.vertex.class";
  public final static String VERTEX_ID_CLASS_ATTR = "hama.graph.vertex.id.class";
  public final static String VERTEX_VALUE_CLASS_ATTR = "hama.graph.vertex.value.class";
  public final static String VERTEX_EDGE_VALUE_CLASS_ATTR = "hama.graph.vertex.edge.value.class";

  public final static String AGGREGATOR_CLASS_ATTR = "hama.graph.aggregator.class";
  public final static String VERTEX_MESSAGE_COMBINER_CLASS_ATTR = "hama.vertex.message.combiner.class";
  public final static String VERTEX_GRAPH_RUNTIME_PARTIONING = "hama.graph.runtime.partitioning";
  public final static String VERTEX_GRAPH_INPUT_READER = "hama.graph.input.reader.class";

  /**
   * Creates a new Graph Job with the given configuration and an exampleClass.
   * The exampleClass is used to determine the user's jar to distribute in the
   * cluster. This constructor sets the vertex id class to {@link Text}, the
   * vertex value class to {@link IntWritable} and the edge value class to
   * {@link IntWritable}.
   */
  public GraphJob(HamaConfiguration conf, Class<?> exampleClass)
      throws IOException {
    super(conf);
    this.setBspClass(GraphJobRunner.class);
    this.setJarByClass(exampleClass);
    this.setVertexIDClass(Text.class);
    this.setVertexValueClass(IntWritable.class);
    this.setEdgeValueClass(IntWritable.class);
    this.setPartitioner(HashPartitioner.class);
  }

  /**
   * Set the Vertex class for the job.
   */
  public void setVertexClass(
      Class<? extends Vertex<? extends Writable, ? extends Writable, ? extends Writable>> cls)
      throws IllegalStateException {
    conf.setClass(VERTEX_CLASS_ATTR, cls, Vertex.class);
  }

  /**
   * Set the Vertex ID class for the job.
   */
  public void setVertexIDClass(Class<? extends Writable> cls)
      throws IllegalStateException {
    conf.setClass(VERTEX_ID_CLASS_ATTR, cls, Writable.class);
  }

  /**
   * Set the Vertex value class for the job.
   */
  public void setVertexValueClass(Class<? extends Writable> cls)
      throws IllegalStateException {
    conf.setClass(VERTEX_VALUE_CLASS_ATTR, cls, Writable.class);
  }

  /**
   * Set the Edge value class for the job.
   */
  public void setEdgeValueClass(Class<? extends Writable> cls)
      throws IllegalStateException {
    conf.setClass(VERTEX_EDGE_VALUE_CLASS_ATTR, cls, Writable.class);
  }

  /**
   * Set the aggregator for the job.
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  public void setAggregatorClass(Class<? extends Aggregator> cls) {
    this.setAggregatorClass(new Class[] { cls });
  }

  /**
   * Sets multiple aggregators for the job.
   */
  @SuppressWarnings("rawtypes")
  public void setAggregatorClass(Class<? extends Aggregator>... cls) {
    String classNames = "";
    for (Class<? extends Aggregator> cl : cls) {
      classNames += cl.getName() + ";";
    }
    conf.set(AGGREGATOR_CLASS_ATTR, classNames);
  }

  /**
   * Sets the input reader for parsing the input to vertices.
   */
  public void setVertexInputReaderClass(
      Class<? extends VertexInputReader<?, ?, ?, ?, ?>> cls) {
    ensureState(JobState.DEFINE);
    conf.setClass(VERTEX_GRAPH_INPUT_READER, cls, VertexInputReader.class);
  }

  @SuppressWarnings("unchecked")
  public Class<? extends Vertex<? extends Writable, ? extends Writable, ? extends Writable>> getVertexClass() {
    return (Class<? extends Vertex<? extends Writable, ? extends Writable, ? extends Writable>>) conf
        .getClass(VERTEX_CLASS_ATTR, Vertex.class);
  }

  @Override
  public void setPartitioner(@SuppressWarnings("rawtypes")
  Class<? extends Partitioner> theClass) {
    super.setPartitioner(theClass);
    conf.setBoolean(VERTEX_GRAPH_RUNTIME_PARTIONING, true);
  }

  @Override
  public void setCombinerClass(Class<? extends Combiner<? extends Writable>> cls) {
    ensureState(JobState.DEFINE);
    conf.setClass(VERTEX_MESSAGE_COMBINER_CLASS_ATTR, cls, Combiner.class);
  }

  /**
   * Sets how many iterations the algorithm should perform, -1 for deactivated
   * is default value.
   */
  public void setMaxIteration(int maxIteration) {
    conf.setInt("hama.graph.max.iteration", maxIteration);
  }

  @Override
  public void submit() throws IOException, InterruptedException {
    Preconditions.checkArgument(
        this.getConfiguration().get(VERTEX_CLASS_ATTR) != null,
        "Please provide a vertex class!");
    Preconditions.checkArgument(
        this.getConfiguration().get(VERTEX_ID_CLASS_ATTR) != null,
        "Please provide an vertex ID class!");
    Preconditions
        .checkArgument(
            this.getConfiguration().get(VERTEX_VALUE_CLASS_ATTR) != null,
            "Please provide an vertex value class, if you don't need one, use NullWritable!");
    Preconditions
        .checkArgument(this.getConfiguration()
            .get(VERTEX_EDGE_VALUE_CLASS_ATTR) != null,
            "Please provide an edge value class, if you don't need one, use NullWritable!");
    Preconditions.checkArgument(
        this.getConfiguration().get(VERTEX_GRAPH_INPUT_READER) != null,
        "Please provide a vertex input reader!");
    super.submit();
  }

}
