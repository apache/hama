/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hama.graph;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hama.bsp.BSPPeer;

/**
 * This class has as a target to iterate the whole sorted queue of the incoming
 * messages. Each vertex will be able to call the <code>hasNext()</code> and
 * <code>next()</code> methods to consume the messages. The iterator is
 * responsible to understand when the messages of a specific Vertex ID have been
 * consumed, and then unlock the messages of the next Vertex ID through the
 * <code>continueWith()</code> method.
 *
 * @param <V>
 * @param <T>
 */
public class VertexMessages<V, T> implements Iterator<T>, Iterable<T> {
  private final BSPPeer<?, ?, ?, ?, GraphJobMessage> peer;
  private V vid = null;
  private GraphJobMessage currentMessage = null;
  private boolean locked = true;

  public VertexMessages(BSPPeer<?, ?, ?, ?, GraphJobMessage> peer) {
    this.peer = peer;
  }

  @Override
  public boolean hasNext() {
    if (locked) {
      return false;
    }

    try {
      if (this.currentMessage == null) {
        this.currentMessage = this.peer.getCurrentMessage();
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    if (this.currentMessage != null && this.currentMessage.getVertexId().equals(this.vid)) {
      return true;
    }
    // When a new ID has shown up or the messages are finished,
    // we lock the iterator
    this.locked = true;
    return false;
  }

  @Override
  public T next() {
    if (this.currentMessage == null || this.locked) {
      return null;
    }

    // Despose the current message and prepare for next hasNext() call
    try {
      return (T) this.currentMessage.getVertexValue();
    } finally {
      this.currentMessage = null;
    }
  }

  /**
   * By implementing both <code>Iterator</code> and <code>Iterable</code>
   * interfaces, this class will not be able to re-iterated and the messages
   * will be accessed only once. In our case this is fine.
   *
   * @return an one-time iterator
   */
  @Override
  public Iterator<T> iterator() {
    return this;
  }

  /**
   * This method should be used only after initialization. If an other message
   * exists in the memory of the iterator, the new prepended message
   * will be ignored.
   *
   * @param msg The message to be prepended just after initialization
   */
  public void prependMessage(GraphJobMessage msg) {
    if (this.currentMessage == null && msg != null) {
      this.currentMessage = msg;
    }
  }

  /**
   * Check the vertexID target of the current message that is loaded in the
   * iterator and unlock the iterator only if the <code>vid</code> argument is
   * matching.
   *
   * @param vid
   * @return return true if the <code>vid</code> is equal to the next message's ID
   */
  public boolean continueWith(V vid) {
    // Normally when we call this method this.locked == true
    this.vid = vid;
    this.locked = false;

    if (this.currentMessage == null) {
      // Get next message (if there is) and decide based on the new vid
      return this.hasNext();
    }

    // If we have a message already loaded
    if (!this.currentMessage.getVertexId().equals(vid)) {
      this.locked = true;
      return false;
    }
    return true;
  }

  /**
   * Consume the incoming messages until we find a message that has a target
   * equal to the <code>vid</code> argument.
   *
   * @param vid
   * @return
   */
  public boolean continueUntil(V vid) {
    do {
      try {
        if (this.currentMessage == null) {
          this.currentMessage = this.peer.getCurrentMessage();
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      if (this.currentMessage == null) {
        this.locked = true;
        return false;
      }
    } while (!this.currentMessage.getVertexId().equals(this.vid));

    this.locked = false;
    return true;
  }

  public void dumpRest() {
    while(this.hasNext()) {
      this.next();
    }
  }

  /**
   * Return the target Vertex ID of the current loaded message.
   *
   * @return
   */
  public V getMessageVID() {
    return this.currentMessage == null ? null : (V) this.currentMessage.getVertexId();
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("remove");
  }
}
