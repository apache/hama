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
package org.apache.hama.commons.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * KeyValueWritable containing a key-value WritableComparable pair.
 * 
 * @param <K> the class of key
 * @param <V> the class of value
 */
@SuppressWarnings("rawtypes")
public class KeyValueWritable<K extends WritableComparable, V extends WritableComparable>
    implements WritableComparable<KeyValueWritable<K, V>> {

  protected K key = null;
  protected V value = null;

  public KeyValueWritable() {
  }

  public KeyValueWritable(K key, V value) {
    this.key = key;
    this.value = value;
  }

  public K getKey() {
    return key;
  }

  public void setKey(K key) {
    this.key = key;
  }

  public V getValue() {
    return value;
  }

  public void setValue(V value) {
    this.value = value;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    key.readFields(in);
    value.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    key.write(out);
    value.write(out);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((key == null) ? 0 : key.hashCode());
    result = prime * result + ((value == null) ? 0 : value.hashCode());
    return result;
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }

    KeyValueWritable<K, V> other = (KeyValueWritable<K, V>) obj;
    if (key == null) {
      if (other.key != null) {
        return false;
      }
    } else if (!key.equals(other.key)) {
      return false;
    }

    if (value == null) {
      if (other.value != null) {
        return false;
      }
    } else if (!value.equals(other.value)) {
      return false;
    }
    return true;
  }

  @SuppressWarnings("unchecked")
  @Override
  public int compareTo(KeyValueWritable<K, V> obj) {
    int cmp = key.compareTo(obj.key);
    if (cmp != 0) {
      return cmp;
    }
    // if keys are equal compare value
    return value.compareTo(obj.value);
  }

}
