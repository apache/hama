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
package org.apache.hama.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HStoreKey;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hama.util.Numeric;

public class VectorMapWritable<K, V> implements Map<Integer, V>, Writable,
    Configurable {
  private AtomicReference<Configuration> conf = new AtomicReference<Configuration>();

  // Static maps of code to class and vice versa. Includes types used in hama
  // only.
  static final Map<Byte, Class<?>> CODE_TO_CLASS = new HashMap<Byte, Class<?>>();
  static final Map<Class<?>, Byte> CLASS_TO_CODE = new HashMap<Class<?>, Byte>();

  static {
    byte code = 0;
    addToMap(HStoreKey.class, code++);
    addToMap(ImmutableBytesWritable.class, code++);
    addToMap(Text.class, code++);
    addToMap(VectorEntry.class, code++);
    addToMap(byte[].class, code++);
  }

  @SuppressWarnings("boxing")
  private static void addToMap(final Class<?> clazz, final byte code) {
    CLASS_TO_CODE.put(clazz, code);
    CODE_TO_CLASS.put(code, clazz);
  }

  private Map<Integer, V> instance = new TreeMap<Integer, V>();

  /** @return the conf */
  public Configuration getConf() {
    return conf.get();
  }

  /** @param conf the conf to set */
  public void setConf(Configuration conf) {
    this.conf.set(conf);
  }

  /** {@inheritDoc} */
  public void clear() {
    instance.clear();
  }

  /** {@inheritDoc} */
  public boolean containsKey(Object key) {
    return instance.containsKey(key);
  }

  /** {@inheritDoc} */
  public boolean containsValue(Object value) {
    return instance.containsValue(value);
  }

  /** {@inheritDoc} */
  public Set<Entry<Integer, V>> entrySet() {
    return instance.entrySet();
  }

  /** {@inheritDoc} */
  public V get(Object key) {
    return instance.get(key);
  }

  /** {@inheritDoc} */
  public boolean isEmpty() {
    return instance.isEmpty();
  }

  /** {@inheritDoc} */
  public Set<Integer> keySet() {
    return instance.keySet();
  }

  /** {@inheritDoc} */
  public int size() {
    return instance.size();
  }

  /** {@inheritDoc} */
  public Collection<V> values() {
    return instance.values();
  }

  // Writable

  /** @return the Class class for the specified id */
  protected Class<?> getClass(byte id) {
    return CODE_TO_CLASS.get(id);
  }

  /** @return the id for the specified Class */
  protected byte getId(Class<?> clazz) {
    Byte b = CLASS_TO_CODE.get(clazz);
    if (b == null) {
      throw new NullPointerException("Nothing for : " + clazz);
    }
    return b;
  }

  @Override
  public String toString() {
    return this.instance.toString();
  }

  /** {@inheritDoc} */
  public void write(DataOutput out) throws IOException {
    // Write out the number of entries in the map
    out.writeInt(this.instance.size());

    // Then write out each key/value pair
    for (Map.Entry<Integer, V> e : instance.entrySet()) {
      Bytes.writeByteArray(out, Numeric.getColumnIndex(e.getKey()));
      out.writeByte(getId(e.getValue().getClass()));
      ((Writable) e.getValue()).write(out);
    }
  }

  /** {@inheritDoc} */
  @SuppressWarnings("unchecked")
public void readFields(DataInput in) throws IOException {
    // First clear the map. Otherwise we will just accumulate
    // entries every time this method is called.
    this.instance.clear();

    // Read the number of entries in the map
    int entries = in.readInt();

    // Then read each key/value pair
    for (int i = 0; i < entries; i++) {
      byte[] key = Bytes.readByteArray(in);
      Writable value = (Writable) ReflectionUtils.newInstance(getClass(in
          .readByte()), getConf());
      value.readFields(in);
      V v = (V) value;
      this.instance.put(Numeric.getColumnIndex(key), v);
    }
  }

  public void putAll(Map<? extends Integer, ? extends V> m) {
    this.instance.putAll(m);
  }

  public V remove(Object key) {
    return this.instance.remove(key);
  }

  public V put(Integer key, V value) {
    return this.instance.put(key, value);
  }
}
