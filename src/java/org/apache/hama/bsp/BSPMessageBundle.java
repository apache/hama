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
import java.util.List;
import org.apache.hadoop.io.Writable;

/**
 * BSPMessageBundle stores a group of BSPMessages so that they can be sent in
 * batch rather than individually.
 * 
 */
public class BSPMessageBundle implements Writable {

  private List<BSPMessage> messages = new ArrayList<BSPMessage>();

  public BSPMessageBundle() {
  }

  /**
   * Add message to this bundle.
   * 
   * @param message BSPMessage to add.
   */
  public void addMessage(BSPMessage message) {
    messages.add(message);
  }

  public List<BSPMessage> getMessages() {
    return messages;
  }

  public void setMessages(List<BSPMessage> messages) {
    this.messages = messages;
  }

  public void write(DataOutput out) throws IOException {
    out.writeInt(messages.size());
    if (messages.size() > 0) {
      // Write out message class.
      BSPMessage element = messages.get(0);
      Class<? extends BSPMessage> clazz = element.getClass();
      out.writeUTF(clazz.getName());
      // Serialize contents of this bundle.
      for (BSPMessage message : messages) {
        message.write(out);
      }
    }
  }

  public void readFields(DataInput in) throws IOException {
    int numMessages = in.readInt();
    if (numMessages > 0) {
      // Get classname of messages.
      String className = in.readUTF();
      Class<? extends BSPMessage> clazz = null;
      try {
        clazz = (Class<? extends BSPMessage>) Class.forName(className);
      } catch (ClassNotFoundException ex) {
        throw new IOException(ex);
      }
      // Deserialize messages.
      messages = new ArrayList<BSPMessage>(numMessages);
      for (int i = 0; i < numMessages; ++i) {
        try {
          // Instantiate new message and deserialize it.
          BSPMessage newMessage = clazz.newInstance();
          newMessage.readFields(in);
          messages.add(newMessage);
        } catch (InstantiationException ex) {
          throw new IOException(ex);
        } catch (IllegalAccessException ex) {
          throw new IOException(ex);
        }
      }
    } else {
      messages = new ArrayList<BSPMessage>();
    }
  }

}
