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
package org.apache.hama.bsp.message.bundle;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;

public class POJOMessageBundle<M extends Writable> implements
    BSPMessageBundle<M>, Iterable<M> {

  protected static final Log LOG = LogFactory.getLog(POJOMessageBundle.class);

  protected HashMap<String, List<M>> messages = new HashMap<String, List<M>>();
  protected HashMap<String, Class<M>> classCache = new HashMap<String, Class<M>>();

  protected int numElements;

  private static class BundleIterator<M extends Writable> implements
      Iterator<M> {

    private Iterator<List<M>> listIterator;
    private Iterator<M> messageIterator;

    public BundleIterator(Iterator<List<M>> listIterator) {
      this.listIterator = listIterator;
    }

    @Override
    public boolean hasNext() {
      return listIterator.hasNext() || messageIterator.hasNext();
    }

    @Override
    public M next() {
      while (true) {
        if (messageIterator != null && messageIterator.hasNext()) {
          return messageIterator.next();
        } else {
          if (listIterator.hasNext()) {
            messageIterator = listIterator.next().iterator();
          } else {
            return null;
          }
        }
      }
    }

    @Override
    public void remove() {
    }

  }

  public POJOMessageBundle() {
  }

  /**
   * Add message to this bundle.
   * 
   * @param message BSPMessage to add.
   */
  public void addMessage(M message) {
    String className = message.getClass().getName();
    List<M> list = messages.get(className);
    ++numElements;
    if (list == null) {
      list = new ArrayList<M>();
      messages.put(className, list);
    }

    list.add(message);
  }

  public List<M> getMessages() {
    // here we use an arraylist, because we know the size and outside may need
    // random access
    List<M> mergeList = new ArrayList<M>(messages.size());
    for (List<M> c : messages.values()) {
      mergeList.addAll(c);
    }
    return mergeList;
  }

  @Override
  public Iterator<M> iterator() {
    return new BundleIterator<M>(this.messages.values().iterator());
  }

  @Override
  public long getSize() {
    return numElements;
  }

  @Override
  public int getNumElements() {
    return numElements;
  }
}
