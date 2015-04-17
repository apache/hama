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
package org.apache.hama.bsp;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;
import org.apache.hama.util.ReflectionUtils;

/**
 * BSPMessageBundle stores a group of messages so that they can be sent in batch
 * rather than individually.
 * 
 */
public class BSPMessageBundle<M extends Writable> implements Writable,
    Iterable<M>, BSPMessageBundleInterface<M> {

  public static final Log LOG = LogFactory.getLog(BSPMessageBundle.class);

  private List<M> messages = new ArrayList<M>();

  public BSPMessageBundle() {
  }

  /**
   * Add message to this bundle.
   * 
   * @param message BSPMessage to add.
   */
  public void addMessage(M message) {
    messages.add(message);
  }

  public void addMessages(Collection<M> msgs) {
    messages.addAll(msgs);
  }

  public Iterator<M> iterator() {
    return messages.iterator();
  }

  public int size() {
    return messages.size();
  }

  @SuppressWarnings("unchecked")
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(messages.size());

    if (messages.size() > 0) {
      Class<M> clazz = (Class<M>) messages.get(0).getClass();
      out.writeUTF(clazz.getName());

      for (M m : messages) {
        m.write(out);
      }
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void readFields(DataInput in) throws IOException {
    int num = in.readInt();

    if (num > 0) {
      Class<M> clazz = null;
      try {
        clazz = (Class<M>) Class.forName(in.readUTF());
      } catch (ClassNotFoundException e) {
        LOG.error("Class was not found.", e);
      }

      for (int i = 0; i < num; i++) {
        M msg = ReflectionUtils.newInstance(clazz);
        msg.readFields(in);
        messages.add(msg);
      }
    }
  }

}
