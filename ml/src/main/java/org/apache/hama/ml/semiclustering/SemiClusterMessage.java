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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hama.graph.Vertex;

/**
 * The SemiClusterMessage class defines the structure of the value stored by
 * each vertex in the graph Job which is same as the Message sent my each
 * vertex.
 * 
 */
public class SemiClusterMessage implements
    WritableComparable<SemiClusterMessage> {

  private String semiClusterId = null;
  private double semiClusterScore = 0.0;

  private List<Vertex<Text, DoubleWritable, SemiClusterMessage>> semiClusterVertexList;
  private Set<SemiClusterDetails> semiClusterContainThis;

  public SemiClusterMessage() {
    semiClusterVertexList = new ArrayList<Vertex<Text, DoubleWritable, SemiClusterMessage>>();
    semiClusterContainThis = new TreeSet<SemiClusterDetails>();
  }

  public double getScore() {
    return semiClusterScore;
  }

  public void setScore(double score) {
    this.semiClusterScore = score;
  }

  public List<Vertex<Text, DoubleWritable, SemiClusterMessage>> getVertexList() {
    return semiClusterVertexList;
  }

  public void addVertex(Vertex<Text, DoubleWritable, SemiClusterMessage> v) {
    this.semiClusterVertexList.add(v);
  }

  public void addVertexList(
      List<Vertex<Text, DoubleWritable, SemiClusterMessage>> list) {
    for (Vertex<Text, DoubleWritable, SemiClusterMessage> v : list) {
      addVertex(v);
    }
  }

  public void setSemiClusterContainThis(
      Set<SemiClusterDetails> semiClusterContainThis) {
    this.semiClusterContainThis = semiClusterContainThis;
  }

  public String getSemiClusterId() {
    return semiClusterId;
  }

  public void setSemiClusterId(String scId) {
    this.semiClusterId = scId;
  }

  public boolean contains(Text vertexID) {
    for (Vertex<Text, DoubleWritable, SemiClusterMessage> v : this.semiClusterVertexList) {
      if (v.getVertexID().equals(vertexID)) {
        return true;
      }
    }
    return false;
  }

  public void readFields(DataInput in) throws IOException {
    if (in.readBoolean()) {
      this.semiClusterId = in.readUTF();
    }
    this.semiClusterScore = in.readDouble();

    if (in.readBoolean()) {
      int len = in.readInt();
      if (len > 0) {
        for (int i = 0; i < len; i++) {
          SemiClusteringVertex v = new SemiClusteringVertex();
          v.readFields(in);
          semiClusterVertexList.add(v);
        }
      }
    }
    int len = in.readInt();
    if (len > 0) {
      for (int i = 0; i < len; i++) {
        SemiClusterDetails sd = new SemiClusterDetails();
        sd.readFields(in);
        semiClusterContainThis.add(sd);
      }
    }

  }

  public void write(DataOutput out) throws IOException {
    if (this.semiClusterId == null) {
      out.writeBoolean(false);
    } else {
      out.writeBoolean(true);
      out.writeUTF(semiClusterId);
    }
    out.writeDouble(semiClusterScore);

    if (this.semiClusterVertexList == null) {
      out.writeBoolean(false);
    } else {
      out.writeBoolean(true);
      out.writeInt(semiClusterVertexList.size());
      for (Vertex<Text, DoubleWritable, SemiClusterMessage> v : semiClusterVertexList) {
        v.write(out);
      }
    }
    out.writeInt(semiClusterContainThis.size());
    for (SemiClusterDetails semiClusterContainThi : semiClusterContainThis)
      semiClusterContainThi.write(out);
  }

  public Set<SemiClusterDetails> getSemiClusterContainThis() {
    return semiClusterContainThis;
  }

  public void setSemiClusterContainThis(
      List<SemiClusterDetails> semiClusterContainThis,
      int graphJobVertexMaxClusterCount) {
    int clusterCountToBeRemoved = 0;
    NavigableSet<SemiClusterDetails> setSort = new TreeSet<SemiClusterDetails>(
        new Comparator<SemiClusterDetails>() {

          @Override
          public int compare(SemiClusterDetails o1, SemiClusterDetails o2) {
            return (o1.getSemiClusterScore() == o2.getSemiClusterScore() ? 0
                : o1.getSemiClusterScore() < o2.getSemiClusterScore() ? -1 : 1);
          }
        });
    setSort.addAll(this.semiClusterContainThis);
    setSort.addAll(semiClusterContainThis);
    clusterCountToBeRemoved = setSort.size() - graphJobVertexMaxClusterCount;
    Iterator<SemiClusterDetails> itr = setSort.descendingIterator();
    while (clusterCountToBeRemoved > 0) {
      itr.next();
      itr.remove();
      clusterCountToBeRemoved--;
    }
    this.semiClusterContainThis = setSort;

  }

  public int compareTo(SemiClusterMessage m) {
    return (this.getSemiClusterId().compareTo(m.getSemiClusterId()));
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result
        + ((semiClusterId == null) ? 0 : semiClusterId.hashCode());
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
    SemiClusterMessage other = (SemiClusterMessage) obj;
    if (semiClusterId == null) {
      if (other.semiClusterId != null)
        return false;
    } else if (!semiClusterId.equals(other.semiClusterId))
      return false;
    return true;
  }

  @Override
  public String toString() {
    return "SCMessage [semiClusterId=" + semiClusterId + ", semiClusterScore="
        + semiClusterScore + ", semiClusterVertexList=" + semiClusterVertexList
        + ", semiClusterContainThis=" + semiClusterContainThis + "]";
  }

  public int size() {
    return this.semiClusterVertexList.size();
  }
}
