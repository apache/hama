/**
 * Copyright 2007 The Apache Software Foundation
 *
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
package org.apache.hama;

import java.util.Iterator;
import java.util.Set;

import org.apache.hama.io.VectorEntry;
import org.apache.hama.io.VectorMapWritable;
import org.apache.log4j.Logger;

public class DenseVector extends AbstractVector implements Vector {
  static final Logger LOG = Logger.getLogger(DenseVector.class);

  public DenseVector() {
    this(new VectorMapWritable<Integer, VectorEntry>());
  }

  public DenseVector(VectorMapWritable<Integer, VectorEntry> m) {
    this.entries = m;
  }

  public Vector add(double alpha, Vector v) {
    if (alpha == 0)
      return this;

    for (int i = 0; i < this.size(); i++) {
      set(i, get(i) + alpha * v.get(i));
    }
    return this;
  }

  public Vector add(Vector v2) {
    if (this.size() == 0) {
      DenseVector trunk = (DenseVector) v2;
      this.entries = trunk.entries;
      return this;
    }

    for (int i = 0; i < this.size(); i++) {
      double value = (this.get(i) + v2.get(i));

      this.entries.put(i, new VectorEntry(value));
    }

    return this;
  }

  public double dot(Vector v) {
    double cosine = 0.0;
    double q_i, d_i;
    for (int i = 0; i < Math.min(this.size(), v.size()); i++) {
      q_i = v.get(i);
      d_i = this.get(i);
      cosine += q_i * d_i;
    }
    return cosine / (this.getNorm2() * ((DenseVector) v).getNorm2());
  }

  public Vector scale(double alpha) {
    Set<Integer> keySet = this.entries.keySet();
    Iterator<Integer> it = keySet.iterator();

    int i = 0;
    while (it.hasNext()) {
      int key = it.next();
      double oValue = this.get(key);
      double nValue = oValue * alpha;

      this.entries.put(i, new VectorEntry(nValue));
      i++;
    }

    return this;
  }

  public double norm(Norm type) {
    if (type == Norm.One)
      return getNorm1();
    else if (type == Norm.Two)
      return getNorm2();
    else if (type == Norm.TwoRobust)
      return getNorm2Robust();
    else
      return getNormInf();
  }

  public DenseVector set(Vector v) {
    return new DenseVector(((DenseVector) v).getEntries());
  }

  public double getNorm1() {
    double sum = 0.0;

    Set<Integer> keySet = this.entries.keySet();
    Iterator<Integer> it = keySet.iterator();

    while (it.hasNext()) {
      sum += get(it.next());
    }

    return sum;
  }

  public double getNorm2() {
    double square_sum = 0.0;

    Set<Integer> keySet = entries.keySet();
    Iterator<Integer> it = keySet.iterator();

    while (it.hasNext()) {
      double value = get(it.next());
      square_sum += value * value;
    }

    return Math.sqrt(square_sum);
  }

  public double getNorm2Robust() {
    // TODO Auto-generated method stub
    return 0;
  }

  public double getNormInf() {
    // TODO Auto-generated method stub
    return 0;
  }

  public Vector subVector(int i0, int i1) {
    Vector res = new DenseVector();
    for (int i = i0; i <= i1; i++) {
      res.set(i, get(i));
    }

    return res;
  }

  public void clear() {
    this.entries = null;
  }
}
