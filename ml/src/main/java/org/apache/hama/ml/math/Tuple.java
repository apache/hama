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
package org.apache.hama.ml.math;

/**
 * Tuple class to hold two generic attributes. This class implements hashcode,
 * equals and comparable via the first element.
 */
public final class Tuple<FIRST, SECOND> implements
    Comparable<Tuple<FIRST, SECOND>> {

  private final FIRST first;
  private final SECOND second;

  public Tuple(FIRST first, SECOND second) {
    super();
    this.first = first;
    this.second = second;
  }

  public final FIRST getFirst() {
    return first;
  }

  public final SECOND getSecond() {
    return second;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((first == null) ? 0 : first.hashCode());
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
    Tuple other = (Tuple) obj;
    if (first == null) {
      if (other.first != null)
        return false;
    } else if (!first.equals(other.first))
      return false;
    return true;
  }

  @SuppressWarnings("unchecked")
  @Override
  public int compareTo(Tuple<FIRST, SECOND> o) {
    if (o.getFirst() instanceof Comparable && getFirst() instanceof Comparable) {
      return ((Comparable<FIRST>) getFirst()).compareTo(o.getFirst());
    } else {
      return 0;
    }
  }

  @Override
  public String toString() {
    return "Tuple [first=" + first + ", second=" + second + "]";
  }

}
