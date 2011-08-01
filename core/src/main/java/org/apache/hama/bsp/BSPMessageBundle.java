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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * BSPMessageBundle stores a group of BSPMessages so that they can be sent in
 * batch rather than individually.
 * 
 */
public class BSPMessageBundle implements Writable {
  
  public static final Log LOG = LogFactory.getLog(BSPMessageBundle.class);

  private HashMap<String, LinkedList<BSPMessage>> messages = new HashMap<String, LinkedList<BSPMessage>>();
  private HashMap<String, Class<? extends BSPMessage>> classCache = new HashMap<String, Class<? extends BSPMessage>>();

  public BSPMessageBundle() {
  }

  /**
   * Add message to this bundle.
   * 
   * @param message BSPMessage to add.
   */
  public void addMessage(BSPMessage message) {
    String className = message.getClass().getName();
    if (!messages.containsKey(className)) {
      // use linked list because we're just iterating over them
      LinkedList<BSPMessage> list = new LinkedList<BSPMessage>();
      list.add(message);
      messages.put(className, list);
    } else {
      messages.get(className).add(message);
    }
  }

  public List<BSPMessage> getMessages() {
    // here we use an arraylist, because we know the size and outside may need
    // random access
    List<BSPMessage> mergeList = new ArrayList<BSPMessage>(messages.size());
    for (LinkedList<BSPMessage> c : messages.values()) {
      mergeList.addAll(c);
    }
    return mergeList;
  }

  public void write(DataOutput out) throws IOException {
    // writes the k/v mapping size
    out.writeInt(messages.size());
    if (messages.size() > 0) {
      for (Entry<String, LinkedList<BSPMessage>> entry : messages.entrySet()) {
        out.writeUTF(entry.getKey());
        LinkedList<BSPMessage> messageList = entry.getValue();
        out.writeInt(messageList.size());
        for (BSPMessage msg : messageList) {
          msg.write(out);
        }
      }
    }
  }

  @SuppressWarnings("unchecked")
  public void readFields(DataInput in) throws IOException {
    if (messages == null) {
      messages = new HashMap<String, LinkedList<BSPMessage>>();
    }
    int numMessages = in.readInt();
    if (numMessages > 0) {
      for (int entries = 0; entries < numMessages; entries++) {
        String className = in.readUTF();
        int size = in.readInt();
        LinkedList<BSPMessage> msgList = new LinkedList<BSPMessage>();
        messages.put(className, msgList);

        Class<? extends BSPMessage> clazz = null;
        if ((clazz = classCache.get(className)) == null) {
          try {
            clazz = (Class<? extends BSPMessage>) Class.forName(className);
            classCache.put(className, clazz);
          } catch (ClassNotFoundException e) {
            LOG.error("Class was not found.",e);
          }
        }

        for (int i = 0; i < size; i++) {
          BSPMessage msg = ReflectionUtils.newInstance(clazz, null);
          msg.readFields(in);
          msgList.add(msg);
        }

      }
    }
  }

}
