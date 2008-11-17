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

import org.apache.hama.io.DoubleEntry;
import org.apache.hama.io.MapWritable;

public abstract class AbstractVector {
  public MapWritable<Integer, DoubleEntry> entries;
  
  public double get(int index) throws NullPointerException {
    return this.entries.get(index).getValue();
  }
  
  public void set(int index, double value) {
    // If entries are null, create new object 
    if(this.entries == null) {
      this.entries = new MapWritable<Integer, DoubleEntry>();
    }
    
    this.entries.put(index, new DoubleEntry(value));
  }
  
  public void add(int index, double value) {
    set(index, get(index) + value);
  }
  
  /**
   * Returns an Iterator.
   * 
   * @return iterator
   */
  public Iterator<DoubleEntry> iterator() {
    return this.entries.values().iterator();
  }
  
  /**
   * Returns a size of vector
   * 
   * @return a size of vector
   */
  public int size() {
    return (this.entries != null) ? this.entries.size() : 0;
  }
  
  public MapWritable<Integer, DoubleEntry> getEntries() {
    return this.entries;
  }
}
