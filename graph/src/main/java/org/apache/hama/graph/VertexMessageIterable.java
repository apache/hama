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
import java.util.Iterator;

import org.apache.hadoop.io.Writable;
import org.apache.hama.bsp.BSPPeer;

import com.google.common.collect.AbstractIterator;

/**
 * The rationale behind this class is that it polls messages if they are
 * requested and once it finds a message that is not dedicated for this vertex,
 * it breaks the iteration. The message that was polled and doesn't belong to
 * the vertex is returned by {@link #getOverflowMessage()}.
 */
public final class VertexMessageIterable<V, T> implements Iterable<T> {

  private final V vertexID;
  private final BSPPeer<Writable, Writable, Writable, Writable, GraphJobMessage> peer;

  private GraphJobMessage overflow;
  private GraphJobMessage currentMessage;

  private Iterator<T> currentIterator;

  public VertexMessageIterable(GraphJobMessage currentMessage, V vertexID,
      BSPPeer<Writable, Writable, Writable, Writable, GraphJobMessage> peer) {
    this.currentMessage = currentMessage;
    this.vertexID = vertexID;
    this.peer = peer;
    setupIterator();
  }

  private void setupIterator() {
    currentIterator = new AbstractIterator<T>() {
      @SuppressWarnings("unchecked")
      @Override
      protected T computeNext() {
        // spool back the current message
        if (currentMessage != null) {
          GraphJobMessage tmp = currentMessage;
          // set it to null, so we don't send it over and over again
          currentMessage = null;
          return (T) tmp.getVertexValue();
        }

        try {
          GraphJobMessage msg = peer.getCurrentMessage();
          if (msg != null) {
            if (msg.getVertexId().equals(vertexID)) {
              return (T) msg.getVertexValue();
            } else {
              overflow = msg;
            }
          }
          return endOfData();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  public GraphJobMessage getOverflowMessage() {
    // check if iterable was completely consumed
    while (currentIterator.hasNext()) {
      currentIterator.next();
    }
    return overflow;
  }

  @Override
  public Iterator<T> iterator() {
    return currentIterator;
  }

}
