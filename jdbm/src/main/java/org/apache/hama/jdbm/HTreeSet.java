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
package org.apache.hama.jdbm;

import java.util.AbstractSet;
import java.util.Iterator;

/**
 * Wrapper for HTree to implement java.util.Map interface
 */
public final class HTreeSet<E> extends AbstractSet<E> {

  final HTree<E, Object> map;

  HTreeSet(HTree<E, Object> map) {
    this.map = map;
  }

  @Override
  public Iterator<E> iterator() {
    return map.keySet().iterator();
  }

  @Override
  public int size() {
    return map.size();
  }

  @Override
  public boolean isEmpty() {
    return map.isEmpty();
  }

  @Override
  public boolean contains(Object o) {
    return map.containsKey(o);
  }

  @Override
  public boolean add(E e) {
    return map.put(e, JDBMUtils.EMPTY_STRING) == null;
  }

  @Override
  public boolean remove(Object o) {
    return map.remove(o) == JDBMUtils.EMPTY_STRING;
  }

  @Override
  public void clear() {
    map.clear();
  }

}
