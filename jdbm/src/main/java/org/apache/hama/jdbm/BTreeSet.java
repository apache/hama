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
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableSet;
import java.util.SortedSet;

/**
 * Wrapper class for <code>>SortedMap</code> to implement
 * <code>>NavigableSet</code>
 * <p/>
 * This code originally comes from Apache Harmony, was adapted by Jan Kotek for
 * JDBM
 */
public final class BTreeSet<E> extends AbstractSet<E> implements
    NavigableSet<E> {

  /**
   * use keyset from this map
   */
  final BTreeMap<E, Object> map;

  BTreeSet(BTreeMap<E, Object> map) {
    this.map = map;
  }

  @Override
  public boolean add(E object) {
    return map.put(object, JDBMUtils.EMPTY_STRING) == null;
  }

  @Override
  public boolean addAll(Collection<? extends E> collection) {
    return super.addAll(collection);
  }

  @Override
  public void clear() {
    map.clear();
  }

  @Override
  public Comparator<? super E> comparator() {
    return map.comparator();
  }

  @Override
  public boolean contains(Object object) {
    return map.containsKey(object);
  }

  @Override
  public boolean isEmpty() {
    return map.isEmpty();
  }

  @Override
  public E lower(E e) {
    return map.lowerKey(e);
  }

  @Override
  public E floor(E e) {
    return map.floorKey(e);
  }

  @Override
  public E ceiling(E e) {
    return map.ceilingKey(e);
  }

  @Override
  public E higher(E e) {
    return map.higherKey(e);
  }

  @Override
  public E pollFirst() {
    Map.Entry<E, Object> e = map.pollFirstEntry();
    return e != null ? e.getKey() : null;
  }

  @Override
  public E pollLast() {
    Map.Entry<E, Object> e = map.pollLastEntry();
    return e != null ? e.getKey() : null;
  }

  @Override
  public Iterator<E> iterator() {
    final Iterator<Map.Entry<E, Object>> iter = map.entrySet().iterator();
    return new Iterator<E>() {
      @Override
      public boolean hasNext() {
        return iter.hasNext();
      }

      @Override
      public E next() {
        Map.Entry<E, Object> e = iter.next();
        return e != null ? e.getKey() : null;
      }

      @Override
      public void remove() {
        iter.remove();
      }
    };
  }

  @Override
  public NavigableSet<E> descendingSet() {
    return map.descendingKeySet();
  }

  @Override
  public Iterator<E> descendingIterator() {
    return map.descendingKeySet().iterator();
  }

  @Override
  public NavigableSet<E> subSet(E fromElement, boolean fromInclusive,
      E toElement, boolean toInclusive) {
    return map.subMap(fromElement, fromInclusive, toElement, toInclusive)
        .navigableKeySet();
  }

  @Override
  public NavigableSet<E> headSet(E toElement, boolean inclusive) {
    return map.headMap(toElement, inclusive).navigableKeySet();
  }

  @Override
  public NavigableSet<E> tailSet(E fromElement, boolean inclusive) {
    return map.tailMap(fromElement, inclusive).navigableKeySet();
  }

  @Override
  public boolean remove(Object object) {
    return map.remove(object) != null;
  }

  @Override
  public int size() {
    return map.size();
  }

  @Override
  public E first() {
    return map.firstKey();
  }

  @Override
  public E last() {
    return map.lastKey();
  }

  @Override
  public SortedSet<E> subSet(E start, E end) {
    Comparator<? super E> c = map.comparator();
    int compare = (c == null) ? ((Comparable<E>) start).compareTo(end) : c
        .compare(start, end);
    if (compare <= 0) {
      return new BTreeSet<E>((BTreeMap<E, Object>) map.subMap(start, true, end,
          false));
    }
    throw new IllegalArgumentException();
  }

  @Override
  public SortedSet<E> headSet(E end) {
    // Check for errors
    Comparator<? super E> c = map.comparator();
    if (c == null) {
      ((Comparable<E>) end).compareTo(end);
    } else {
      c.compare(end, end);
    }
    return new BTreeSet<E>((BTreeMap<E, Object>) map.headMap(end, false));
  }

  @Override
  public SortedSet<E> tailSet(E start) {
    // Check for errors
    Comparator<? super E> c = map.comparator();
    if (c == null) {
      ((Comparable<E>) start).compareTo(start);
    } else {
      c.compare(start, start);
    }
    return new BTreeSet<E>((BTreeMap<E, Object>) map.tailMap(start, true));
  }

}
