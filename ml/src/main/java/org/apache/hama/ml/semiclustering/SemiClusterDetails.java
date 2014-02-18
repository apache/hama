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

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * The SemiClusterDetails class is used to store a Semi-Cluster ID and its
 * score.This class implements Comparable interface which compares the score of
 * the objects.
 * 
 */

public class SemiClusterDetails implements
    WritableComparable<SemiClusterDetails> {

  private String semiClusterId;
  private double semiClusterScore;

  public SemiClusterDetails() {
    this.semiClusterId = "";
    this.semiClusterScore = 1.0;
  }

  public SemiClusterDetails(String semiClusterId, double semiClusterScore) {
    this.semiClusterId = semiClusterId;
    this.semiClusterScore = semiClusterScore;
  }

  public String getSemiClusterId() {
    return semiClusterId;
  }

  public void setSemiClusterId(String semiClusterId) {
    this.semiClusterId = semiClusterId;
  }

  public double getSemiClusterScore() {
    return semiClusterScore;
  }

  public void setSemiClusterScore(double semiClusterScore) {
    this.semiClusterScore = semiClusterScore;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result
        + ((semiClusterId == null) ? 0 : semiClusterId.hashCode());
    long temp;
    temp = Double.doubleToLongBits(semiClusterScore);
    result = prime * result + (int) (temp ^ (temp >>> 32));
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
    SemiClusterDetails other = (SemiClusterDetails) obj;
    if (semiClusterId == null) {
      if (other.semiClusterId != null)
        return false;
    } else if (!semiClusterId.equals(other.semiClusterId))
      return false;
    return Double.doubleToLongBits(semiClusterScore) == Double
        .doubleToLongBits(other.semiClusterScore);
  }

  @Override
  public String toString() {
    return semiClusterId;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.semiClusterId = in.readUTF();
    this.semiClusterScore = in.readDouble();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(semiClusterId);
    out.writeDouble(semiClusterScore);
  }

  @Override
  public int compareTo(SemiClusterDetails sc) {
    return (this.getSemiClusterId().compareTo(sc.getSemiClusterId()));
  }
}
