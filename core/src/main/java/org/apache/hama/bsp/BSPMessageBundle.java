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
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * BSPMessageBundle stores a group of messages so that they can be sent in batch
 * rather than individually.
 * 
 */
public class BSPMessageBundle<M extends Writable> implements Writable,
    Iterable<M>, BSPMessageBundleInterface<M> {

  public static final Log LOG = LogFactory.getLog(BSPMessageBundle.class);

  private String className = null;
  private int bundleSize = 0;

  private Kryo kryo = new Kryo();
  private ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
  private Output output = new Output(outputStream, 4096);

  public BSPMessageBundle() {
    bundleSize = 0;
  }

  /**
   * Add message to this bundle.
   * 
   * @param message BSPMessage to add.
   */
  public void addMessage(M message) {
    if (className == null) {
      className = message.getClass().getName();
      kryo.register(message.getClass());
    }

    kryo.writeObject(output, message);
    output.flush();

    bundleSize++;
  }

  public byte[] getBuffer() {
    return outputStream.toByteArray();
  }

  private ByteArrayInputStream bis = null;
  private Input in = null;

  public Iterator<M> iterator() {
    bis = new ByteArrayInputStream(outputStream.toByteArray());
    in = new Input(bis, 4096);

    Iterator<M> it = new Iterator<M>() {
      Class<M> clazz = null;
      int counter = 0;

      @Override
      public boolean hasNext() {
        if ((bundleSize - counter) > 0) {
          return true;
        } else {
          return false;
        }
      }

      @SuppressWarnings("unchecked")
      @Override
      public M next() {
        try {
          if (clazz == null) {
            clazz = (Class<M>) Class.forName(className);
          }
        } catch (ClassNotFoundException ce) {
          LOG.error("Class was not found.", ce);
        }

        counter++;

        return kryo.readObject(in, clazz);
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
   * @return the byte length of messages
   * @throws IOException
   */
  public long getLength() {
    return outputStream.size();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(bundleSize);
    if (bundleSize > 0) {
      out.writeUTF(className);
      out.writeInt(outputStream.size());
      out.write(outputStream.toByteArray());
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.bundleSize = in.readInt();

    if (this.bundleSize > 0) {
      className = in.readUTF();
      int bytesLength = in.readInt();
      byte[] temp = new byte[bytesLength];
      in.readFully(temp);
      outputStream.write(temp);
    }
  }

}
