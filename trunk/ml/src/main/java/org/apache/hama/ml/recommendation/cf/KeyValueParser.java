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
package org.apache.hama.ml.recommendation.cf;

import org.apache.hadoop.io.Writable;

/**
 * Take line and convert it to Key Value,
 * class contains "K key" 
 * and "V value" variables
 *
 * @param <K> - key
 * @param <V> - value
 */
public abstract class KeyValueParser<K extends Writable, V extends Writable> {
  protected K key = null;
  protected V value = null;
  public abstract void parseLine(String ln);

  /**
   * 
   * @return key, if not parsed yet may return null
   * or value from last parsed line
   */
  public K getKey() {
    return key;
  }
  
  /**
   * 
   * @return value, if not parsed yet may return null
   * or value from last parsed line
   */
  public V getValue() {
    return value;
  }
}

  