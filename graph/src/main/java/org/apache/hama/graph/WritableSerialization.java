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
package org.apache.hama.graph;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hama.jdbm.Serializer;

/**
 * Writable serialization for Hadoop objects with a class cache to reduce the
 * amount of writing the classname instead of a single integer (with V
 * compression, so most of the time it just takes a single byte).
 */
public class WritableSerialization<K extends Writable> implements
    Serializer<K>, Serializable {

  private static final long serialVersionUID = 1L;

  // clazzname as string -> index in the lookuplist
  protected transient final HashMap<String, Integer> CLAZZ_CACHE = new HashMap<String, Integer>();
  protected transient final ArrayList<Class<?>> LOOKUP_LIST = new ArrayList<Class<?>>();
  private transient int lastAssigned = 0;

  protected transient Writable instance;
  protected transient int writableClassIndex;

  public WritableSerialization() {
  }

  public WritableSerialization(Class<?> writableClazz) {
    Integer integer = CLAZZ_CACHE.get(writableClazz);
    if (integer == null) {
      integer = lastAssigned++;
      CLAZZ_CACHE.put(writableClazz.getName(), integer);
      LOOKUP_LIST.add(writableClazz);
    }
    this.writableClassIndex = integer;
  }

  @Override
  public void serialize(DataOutput out, K obj) throws IOException {
    WritableUtils.writeVInt(out, writableClassIndex);
    obj.write(out);
  }

  @SuppressWarnings("unchecked")
  @Override
  public K deserialize(DataInput in) throws IOException, ClassNotFoundException {
    writableClassIndex = WritableUtils.readVInt(in);
    instance = newInstance();
    instance.readFields(in);
    return (K) instance;
  }

  public Writable newInstance() {
    return (Writable) ReflectionUtils.newInstance(
        LOOKUP_LIST.get(writableClassIndex), null);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((instance == null) ? 0 : instance.hashCode());
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
    @SuppressWarnings("rawtypes")
    WritableSerialization other = (WritableSerialization) obj;
    if (instance == null) {
      if (other.instance != null)
        return false;
    } else if (!instance.equals(other.instance))
      return false;
    return true;
  }

}
