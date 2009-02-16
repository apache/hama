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

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hama.io.DoubleEntry;

/**
 * Methods of the vector classes
 */
public abstract class AbstractVector {
  protected MapWritable entries;
  
  /**
   * Gets the value of index
   * 
   * @param index
   * @return the value of v(index)
   */
  public double get(int index) {
    double value;
    try {
      value = ((DoubleEntry) this.entries.get(new IntWritable(index))).getValue();
    } catch (NullPointerException e) {
      throw new NullPointerException("v("+index+") : " + e.toString());
    }
    
    return value;
  }
  
  /**
   * Sets the value of index
   * 
   * @param index
   * @param value
   */
  public void set(int index, double value) {
    // If entries are null, create new object 
    if(this.entries == null) {
      this.entries = new MapWritable();
    }
    
    this.entries.put(new IntWritable(index), new DoubleEntry(value));
  }
  
  /**
   * Adds the value to v(index)
   * 
   * @param index
   * @param value
   */
  public void add(int index, double value) {
    set(index, get(index) + value);
  }
  
  /**
   * Returns an Iterator.
   * 
   * @return iterator
   */
  public Iterator<Writable> iterator() {
    return this.entries.values().iterator();
  }
  
  /**
   * Returns a size of vector
   * 
   * @return a size of vector
   */
  public int size() {
    int x = 0;
    if(this.entries != null && this.entries.containsKey(new Text("row"))) 
      x = 1;
    
    return (this.entries != null) ? this.entries.size() - x : 0;
  }
  
  /**
   * Returns the {@link org.apache.hadoop.io.MapWritable}
   * 
   * @return the entries of vector
   */
  public MapWritable getEntries() {
    return this.entries;
  }
}
