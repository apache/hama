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

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class VertexWritable<VERTEX_ID, VERTEX_VALUE> implements
    WritableComparable<VertexWritable<VERTEX_ID, VERTEX_VALUE>>, Configurable {

  /**
   * This field is static because it doesn't need to be an instance variable. It
   * is written in upper case, because it is considered constant per launched
   * process.
   */
  public static Configuration CONFIGURATION;

  VERTEX_ID vertexId;
  VERTEX_VALUE value;
  Class<VERTEX_ID> idCls;
  Class<VERTEX_VALUE> valCls;

  public VertexWritable() {
    super();
  }

  @SuppressWarnings("unchecked")
  public VertexWritable(VERTEX_ID name, Class<VERTEX_ID> idCls) {
    this.vertexId = name;
    this.value = (VERTEX_VALUE) new IntWritable(0);
    this.idCls = idCls;
    this.valCls = org.apache.hadoop.util.ReflectionUtils.getClass(value);
  }

  @SuppressWarnings("unchecked")
  public VertexWritable(int weight, String name) {
    this.vertexId = (VERTEX_ID) new Text(name);
    this.value = (VERTEX_VALUE) new IntWritable(weight);
    this.idCls = org.apache.hadoop.util.ReflectionUtils.getClass(vertexId);
    this.valCls = org.apache.hadoop.util.ReflectionUtils.getClass(value);
  }

  @SuppressWarnings("unchecked")
  public VertexWritable(String name) {
    this.vertexId = (VERTEX_ID) new Text(name);
    this.value = (VERTEX_VALUE) NullWritable.get();
    this.idCls = org.apache.hadoop.util.ReflectionUtils.getClass(vertexId);
    this.valCls = org.apache.hadoop.util.ReflectionUtils.getClass(value);
  }

  public VertexWritable(VERTEX_VALUE weight, VERTEX_ID name,
      Class<VERTEX_ID> idCls, Class<VERTEX_VALUE> valCls) {
    this.vertexId = name;
    this.value = weight;
    this.idCls = idCls;
    this.valCls = valCls;
  }

  public VERTEX_ID getVertexId() {
    return vertexId;
  }

  public VERTEX_VALUE getVertexValue() {
    return value;
  }

  @Override
  public String toString() {
    return getVertexId().toString();
  }

  @SuppressWarnings("unchecked")
  @Override
  public void readFields(DataInput in) throws IOException {
    try {
      idCls = (Class<VERTEX_ID>) CONFIGURATION.getClassByName(in.readUTF());
      valCls = (Class<VERTEX_VALUE>) CONFIGURATION.getClassByName(in.readUTF());
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    }
    vertexId = (VERTEX_ID) ObjectWritable.readObject(in, CONFIGURATION);
    value = (VERTEX_VALUE) ObjectWritable.readObject(in, CONFIGURATION);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(idCls.getName());
    out.writeUTF(valCls.getName());
    ObjectWritable.writeObject(out, vertexId, idCls, CONFIGURATION);
    ObjectWritable.writeObject(out, value, valCls, CONFIGURATION);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((vertexId == null) ? 0 : vertexId.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    @SuppressWarnings("unchecked")
    VertexWritable<VERTEX_ID, VERTEX_VALUE> other = (VertexWritable<VERTEX_ID, VERTEX_VALUE>) obj;
    if (vertexId == null) {
      if (other.vertexId != null)
        return false;
    } else if (!vertexId.equals(other.vertexId))
      return false;
    return true;
  }

  @SuppressWarnings("unchecked")
  @Override
  public int compareTo(VertexWritable<VERTEX_ID, VERTEX_VALUE> o) {
    VertexWritable<VERTEX_ID, VERTEX_VALUE> that = o;
    return ((Comparable<VertexWritable<VERTEX_ID, VERTEX_VALUE>>) this.vertexId)
        .compareTo((VertexWritable<VERTEX_ID, VERTEX_VALUE>) that.vertexId);
  }

  @Override
  public void setConf(Configuration conf) {
    VertexWritable.CONFIGURATION = conf;
  }

  @Override
  public Configuration getConf() {
    return CONFIGURATION;
  }

}
