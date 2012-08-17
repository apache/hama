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

package org.apache.hama.util;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Simple LinkedHashMap based LRU Cache for specified capacity.
 * 
 * @param <K> the type of keys maintained by this map
 * @param <V> the type of mapped values
 */
public abstract class LRUCache<K, V> extends LinkedHashMap<K, V> {

  private static final long serialVersionUID = -3347750474082019514L;
  
  /**
   * The maximum size of the cache.
   */
  protected final int capacity;

  /**
   * Constructs an empty <tt>LinkedHashMap</tt> instance. Cache Capacity
   * argument is used to ensure the size of the cache.
   * 
   * @param cacheCapacity
   */
  protected LRUCache(int cacheCapacity) {
    super();
    this.capacity = cacheCapacity;
  }

  /**
   * @param eldest
   * @return <tt>true</tt> if element insertion increases the size of the cache
   *         than cache capacity.
   */
  @Override
  protected abstract boolean removeEldestEntry(Map.Entry<K, V> eldest);
}
