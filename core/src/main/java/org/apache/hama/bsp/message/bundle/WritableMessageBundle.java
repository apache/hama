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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

public class WritableMessageBundle<M extends Writable> extends
    POJOMessageBundle<M> implements Writable {

  @Override
  public void write(DataOutput out) throws IOException {
    // writes the k/v mapping size
    out.writeInt(messages.size());
    if (messages.size() > 0) {
      for (Entry<String, List<M>> entry : messages.entrySet()) {
        out.writeUTF(entry.getKey());
        List<M> messageList = entry.getValue();
        out.writeInt(messageList.size());
        for (M msg : messageList) {
          msg.write(out);
        }
      }
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public void readFields(DataInput in) throws IOException {
    if (messages == null) {
      messages = new HashMap<String, List<M>>();
    }
    int numMessages = in.readInt();
    if (numMessages > 0) {
      for (int entries = 0; entries < numMessages; entries++) {
        String className = in.readUTF();
        int size = in.readInt();
        List<M> msgList = new ArrayList<M>(size);
        messages.put(className, msgList);

        Class<M> clazz = null;
        if ((clazz = classCache.get(className)) == null) {
          try {
            clazz = (Class<M>) Class.forName(className);
            classCache.put(className, clazz);
          } catch (ClassNotFoundException e) {
            LOG.error("Class was not found.", e);
          }
        }

        for (int i = 0; i < size; i++) {
          M msg = ReflectionUtils.newInstance(clazz, null);
          msg.readFields(in);
          msgList.add(msg);
        }

      }
    }
  }

}
