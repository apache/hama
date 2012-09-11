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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * A message that is either MapWritable (for meta communication purposes) or a
 * real message (vertex ID and value). It can be extended by adding flags, for
 * example for a graph repair call.
 */
public final class GraphJobMessage implements Writable {

  public static final int MAP_FLAG = 0x01;
  public static final int VERTEX_FLAG = 0x02;
  public static final int REPAIR_FLAG = 0x04;
  public static final int PARTITION_FLAG = 0x08;
  public static final int VERTICES_SIZE_FLAG = 0x10;

  // staticly defined because it is process-wide information, therefore in caps
  // considered as a constant
  public static Class<?> VERTEX_CLASS;
  public static Class<? extends Writable> VERTEX_ID_CLASS;
  public static Class<? extends Writable> VERTEX_VALUE_CLASS;
  public static Class<? extends Writable> EDGE_VALUE_CLASS;

  // default flag to -1 "unknown"
  private int flag = -1;
  private MapWritable map;
  private Writable vertexId;
  private Writable vertexValue;
  private Vertex<?, ?, ?> vertex;
  private IntWritable vertices_size;

  public GraphJobMessage() {
  }

  public GraphJobMessage(MapWritable map) {
    this.flag = MAP_FLAG;
    this.map = map;
  }

  public GraphJobMessage(Writable vertexId) {
    this.flag = REPAIR_FLAG;
    this.vertexId = vertexId;
  }

  public GraphJobMessage(Writable vertexId, Writable vertexValue) {
    this.flag = VERTEX_FLAG;
    this.vertexId = vertexId;
    this.vertexValue = vertexValue;
  }

  public GraphJobMessage(Vertex<?, ?, ?> vertex) {
    this.flag = PARTITION_FLAG;
    this.vertex = vertex;
  }

  public GraphJobMessage(IntWritable size) {
    this.flag = VERTICES_SIZE_FLAG;
    this.vertices_size = size;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeByte(this.flag);
    if (isVertexMessage()) {
      // we don't need to write the classes because the other side has the same
      // classes for the two entities.
      vertexId.write(out);
      vertexValue.write(out);
    } else if (isMapMessage()) {
      map.write(out);
    } else if (isPartitioningMessage()) {
      vertex.getVertexID().write(out);
      if (vertex.getValue() != null) {
        out.writeBoolean(true);
        vertex.getValue().write(out);
      } else {
        out.writeBoolean(false);
      }
      List<?> outEdges = vertex.getEdges();
      out.writeInt(outEdges.size());
      for (Object e : outEdges) {
        Edge<?, ?> edge = (Edge<?, ?>) e;
        edge.getDestinationVertexID().write(out);
        if (edge.getValue() != null) {
          out.writeBoolean(true);
          edge.getValue().write(out);
        } else {
          out.writeBoolean(false);
        }
      }
    } else if (isVerticesSizeMessage()) {
      vertices_size.write(out);
    } else {
      vertexId.write(out);
    }

  }

  @Override
  public void readFields(DataInput in) throws IOException {
    flag = in.readByte();
    if (isVertexMessage()) {
      vertexId = ReflectionUtils.newInstance(VERTEX_ID_CLASS, null);
      vertexId.readFields(in);
      vertexValue = ReflectionUtils.newInstance(VERTEX_VALUE_CLASS, null);
      vertexValue.readFields(in);
    } else if (isMapMessage()) {
      map = new MapWritable();
      map.readFields(in);
    } else if (isPartitioningMessage()) {
      Vertex<Writable, Writable, Writable> vertex = GraphJobRunner
          .newVertexInstance(VERTEX_CLASS, null);
      Writable vertexId = ReflectionUtils.newInstance(VERTEX_ID_CLASS, null);
      vertexId.readFields(in);
      vertex.setVertexID(vertexId);
      if (in.readBoolean()) {
        Writable vertexValue = ReflectionUtils.newInstance(VERTEX_VALUE_CLASS,
            null);
        vertexValue.readFields(in);
        vertex.setValue(vertexValue);
      }
      int size = in.readInt();
      vertex.setEdges(new ArrayList<Edge<Writable, Writable>>(size));
      for (int i = 0; i < size; i++) {
        Writable edgeVertexID = ReflectionUtils.newInstance(VERTEX_ID_CLASS,
            null);
        edgeVertexID.readFields(in);
        Writable edgeValue = null;
        if (in.readBoolean()) {
          edgeValue = ReflectionUtils.newInstance(EDGE_VALUE_CLASS, null);
          edgeValue.readFields(in);
        }
        vertex.getEdges().add(
            new Edge<Writable, Writable>(edgeVertexID, edgeValue));
      }
      this.vertex = vertex;
    } else if (isVerticesSizeMessage()) {
      vertices_size = new IntWritable();
      vertices_size.readFields(in);
    } else {
      vertexId = ReflectionUtils.newInstance(VERTEX_ID_CLASS, null);
      vertexId.readFields(in);
    }
  }

  public MapWritable getMap() {
    return map;
  }

  public Writable getVertexId() {
    return vertexId;
  }

  public Writable getVertexValue() {
    return vertexValue;
  }

  public Vertex<?, ?, ?> getVertex() {
    return vertex;
  }

  public IntWritable getVerticesSize() {
    return vertices_size;
  }

  public boolean isMapMessage() {
    return flag == MAP_FLAG;
  }

  public boolean isVertexMessage() {
    return flag == VERTEX_FLAG;
  }

  public boolean isRepairMessage() {
    return flag == REPAIR_FLAG;
  }

  public boolean isPartitioningMessage() {
    return flag == PARTITION_FLAG;
  }

  public boolean isVerticesSizeMessage() {
    return flag == VERTICES_SIZE_FLAG;
  }

  @Override
  public String toString() {
    return "GraphJobMessage [flag=" + flag + ", map=" + map + ", vertexId="
        + vertexId + ", vertexValue=" + vertexValue + ", vertex=" + vertex
        + "]";
  }

}
