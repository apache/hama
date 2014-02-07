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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * BSPMessageBundle stores a group of messages so that they can be sent in batch
 * rather than individually.
 * 
 */
public class BSPMessageBundle<M extends Writable> implements Writable,
    Iterable<M> {

  public static final Log LOG = LogFactory.getLog(BSPMessageBundle.class);

  private String className = null;
  private int bundleSize = 0;

  ByteArrayOutputStream bos = null;
  DataOutputStream dos = null;
  ByteArrayInputStream bis = null;
  DataInputStream dis = null;

  public BSPMessageBundle() {
    bos = new ByteArrayOutputStream();
    dos = new DataOutputStream(bos);
    bundleSize = 0;
  }

  /**
   * Add message to this bundle.
   * 
   * @param message BSPMessage to add.
   */
  public void addMessage(M message) {
    try {
      message.write(dos);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    if (className == null) {
      className = message.getClass().getName();
    }
    bundleSize++;
  }

  public Iterator<M> iterator() {
    Iterator<M> it = new Iterator<M>() {
      ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
      DataInputStream dis = new DataInputStream(bis);
      M msg;

      @Override
      public boolean hasNext() {
        try {
          if (dis.available() > 0) {
            return true;
          } else {
            return false;
          }
        } catch (IOException e) {
          return false;
        }
      }

      @SuppressWarnings("unchecked")
      @Override
      public M next() {
        Class<M> clazz = null;
        try {
          clazz = (Class<M>) Class.forName(className);
        } catch (ClassNotFoundException e) {
          LOG.error("Class was not found.", e);
        }
        msg = ReflectionUtils.newInstance(clazz, null);

        try {
          msg.readFields(dis);
        } catch (IOException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }

        return msg;
      }

      @Override
      public void remove() {
        // TODO Auto-generated method stub
      }
    };
    return it;
  }

  public int size() {
    return bundleSize;
  }

  /**
   * @return the byte length of bundle object
   * @throws IOException
   */
  public long getLength() throws IOException {
    return bos.toByteArray().length;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(bundleSize);
    if (bundleSize > 0) {
      out.writeUTF(className);
      byte[] messages = bos.toByteArray();
      out.writeInt(messages.length);
      out.write(messages);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int numMessages = in.readInt();
    if (numMessages > 0) {
      className = in.readUTF();
      int bytesLength = in.readInt();
      byte[] temp = new byte[bytesLength];
      in.readFully(temp);
      dos.write(temp);
    }
  }
}
