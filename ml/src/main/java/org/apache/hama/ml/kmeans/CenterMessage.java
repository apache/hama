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
package org.apache.hama.ml.kmeans;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hama.ml.math.DoubleVector;
import org.apache.hama.ml.writable.VectorWritable;

public final class CenterMessage implements Writable {

  private int centerIndex;
  private DoubleVector newCenter;
  private int incrementCounter;

  public CenterMessage() {
  }

  public CenterMessage(int key, DoubleVector value) {
    this.centerIndex = key;
    this.newCenter = value;
  }

  public CenterMessage(int key, int increment, DoubleVector value) {
    this.centerIndex = key;
    this.incrementCounter = increment;
    this.newCenter = value;
  }

  @Override
  public final void readFields(DataInput in) throws IOException {
    centerIndex = in.readInt();
    incrementCounter = in.readInt();
    newCenter = VectorWritable.readVector(in);
  }

  @Override
  public final void write(DataOutput out) throws IOException {
    out.writeInt(centerIndex);
    out.writeInt(incrementCounter);
    VectorWritable.writeVector(newCenter, out);
  }

  public int getCenterIndex() {
    return centerIndex;
  }

  public int getIncrementCounter() {
    return incrementCounter;
  }

  public final int getTag() {
    return centerIndex;
  }

  public final DoubleVector getData() {
    return newCenter;
  }

}
