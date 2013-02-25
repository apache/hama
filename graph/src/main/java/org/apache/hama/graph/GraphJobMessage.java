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

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * A message that is either MapWritable (for meta communication purposes) or a
 * real message (vertex ID and value). It can be extended by adding flags, for
 * example for a graph repair call.
 */
public final class GraphJobMessage implements
    WritableComparable<GraphJobMessage> {

  public static final int MAP_FLAG = 0x01;
  public static final int VERTEX_FLAG = 0x02;
  public static final int VERTICES_SIZE_FLAG = 0x04;

  // default flag to -1 "unknown"
  private int flag = -1;
  private MapWritable map;
  @SuppressWarnings("rawtypes")
  private WritableComparable vertexId;
  private Writable vertexValue;
  private IntWritable verticesSize;

  public GraphJobMessage() {
  }

  public GraphJobMessage(MapWritable map) {
    this.flag = MAP_FLAG;
    this.map = map;
  }

  public GraphJobMessage(WritableComparable<?> vertexId, Writable vertexValue) {
    this.flag = VERTEX_FLAG;
    this.vertexId = vertexId;
    this.vertexValue = vertexValue;
  }

  public GraphJobMessage(IntWritable size) {
    this.flag = VERTICES_SIZE_FLAG;
    this.verticesSize = size;
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
    } else if (isVerticesSizeMessage()) {
      verticesSize.write(out);
    } else {
      vertexId.write(out);
    }

  }

  @Override
  public void readFields(DataInput in) throws IOException {
    flag = in.readByte();
    if (isVertexMessage()) {
      vertexId = GraphJobRunner.createVertexIDObject();
      vertexId.readFields(in);
      vertexValue = GraphJobRunner.createVertexValue();
      vertexValue.readFields(in);
    } else if (isMapMessage()) {
      map = new MapWritable();
      map.readFields(in);
    } else if (isVerticesSizeMessage()) {
      verticesSize = new IntWritable();
      verticesSize.readFields(in);
    } else {
      vertexId = ReflectionUtils.newInstance(GraphJobRunner.VERTEX_ID_CLASS,
          null);
      vertexId.readFields(in);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public int compareTo(GraphJobMessage that) {
    if (this.flag != that.flag) {
      return (this.flag - that.flag);
    } else {
      if (this.isVertexMessage()) {
        return this.vertexId.compareTo(that.vertexId);
      } else if (this.isMapMessage()) {
        return Integer.MIN_VALUE;
      }
    }
    return 0;
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

  public IntWritable getVerticesSize() {
    return verticesSize;
  }

  public boolean isMapMessage() {
    return flag == MAP_FLAG;
  }

  public boolean isVertexMessage() {
    return flag == VERTEX_FLAG;
  }

  public boolean isVerticesSizeMessage() {
    return flag == VERTICES_SIZE_FLAG;
  }

  @Override
  public String toString() {
    if (isVertexMessage()) {
      return "ID: " + vertexId + " Val: " + vertexValue;
    } else if (isMapMessage()) {
      return "Map: " + map;
    } else if (isVerticesSizeMessage()) {
      return "#Vertices: " + verticesSize;
    } else {
      return "GraphJobMessage [flag=" + flag + ", map=" + map + ", vertexId="
          + vertexId + ", vertexValue=" + vertexValue + "]";
    }
  }

}
