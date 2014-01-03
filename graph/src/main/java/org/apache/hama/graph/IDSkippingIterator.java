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

import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Iterator that allows skipping of items on disk based on some given stategy.
 */
@SuppressWarnings("rawtypes")
public abstract class IDSkippingIterator<V extends WritableComparable, E extends Writable, M extends Writable> {

  enum Strategy {
    ALL, ACTIVE, ACTIVE_AND_MESSAGES, INACTIVE;

    // WritableComparable is really sucking in this type constellation
    @SuppressWarnings("unchecked")
    public boolean accept(Vertex v, WritableComparable msgId) {
      switch (this) {
        case ACTIVE_AND_MESSAGES:
          if (msgId != null) {
            return !v.isHalted() || v.getVertexID().compareTo(msgId) == 0;
          }
          // fallthrough to activeness if we don't have a message anymore
        case ACTIVE:
          return !v.isHalted();
        case INACTIVE:
          return v.isHalted();
        case ALL:
          // fall through intended
        default:
          return true;
      }
    }
  }

  /**
   * Skips nothing, accepts everything.
   * 
   * @return true if the strategy found a new item, false if not.
   * @throws IOException 
   */
  public boolean hasNext() throws IOException {
    return hasNext(null, Strategy.ALL);
  }

  /**
   * Skips until the given strategy is satisfied.
   * 
   * @return true if the strategy found a new item, false if not.
   * @throws IOException 
   */
  public abstract boolean hasNext(V e, Strategy strat) throws IOException;

  /**
   * @return a found vertex that can be read safely.
   */
  public abstract Vertex<V, E, M> next();
}
