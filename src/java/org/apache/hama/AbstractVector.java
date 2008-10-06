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

import org.apache.hama.io.VectorEntry;
import org.apache.hama.io.VectorMapWritable;

public abstract class AbstractVector {
  public VectorMapWritable<Integer, VectorEntry> entries;
  
  public double get(int index) {
    return this.entries.get(index).getValue();
  }
  
  public void add(int index, double value) {
    // TODO Auto-generated method stub
  }
  
  /**
   * Returns an Iterator.
   * 
   * @return iterator
   */
  public Iterator<VectorEntry> iterator() {
    return this.entries.values().iterator();
  }
  
  /**
   * Returns a size of vector
   * 
   * @return a size of vector
   */
  public int size() {
    return this.entries.size();
  }
  
  public VectorMapWritable<Integer, VectorEntry> getEntries() {
    return this.entries;
  }
}
