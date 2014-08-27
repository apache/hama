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
import org.apache.hama.bsp.message.compress.BSPMessageCompressor;

/**
 * BSPMessageBundle stores a group of messages so that they can be sent in batch
 * rather than individually.
 * 
 */
public class BSPMessageBundle<M extends Writable> implements Writable,
    Iterable<M>, BSPMessageBundleInterface<M> {

  public static final Log LOG = LogFactory.getLog(BSPMessageBundle.class);

  private BSPMessageCompressor<M> compressor = null;
  private long threshold = 128;

  private String className = null;
  private int bundleSize = 0;

  private final ByteArrayOutputStream byteBuffer = new ByteArrayOutputStream();
  private final DataOutputStream bufferDos = new DataOutputStream(byteBuffer);

  public BSPMessageBundle() {
    bundleSize = 0;
  }

  ByteArrayOutputStream mbos = new ByteArrayOutputStream();
  DataOutputStream mdos = new DataOutputStream(mbos);
  ByteArrayInputStream mbis = null;
  DataInputStream mdis = null;

  public byte[] serialize(M message) throws IOException {
    mbos.reset();
    message.write(mdos);
    return mbos.toByteArray();
  }

  private byte[] msgBytes;

  /**
   * Add message to this bundle.
   * 
   * @param message BSPMessage to add.
   */
  public void addMessage(M message) {
    try {
      if (className == null) {
        className = message.getClass().getName();
      }

      msgBytes = serialize(message);

      if (compressor != null) {
        if (msgBytes.length > threshold) {
          bufferDos.writeBoolean(true);
          msgBytes = compressor.compress(msgBytes);
          bufferDos.writeInt(msgBytes.length);
        } else {
          bufferDos.writeBoolean(false);
        }
      }
      bufferDos.write(msgBytes);
      bundleSize++;
    } catch (IOException e) {
      LOG.error(e);
    }
  }

  public byte[] getBuffer() {
    return byteBuffer.toByteArray();
  }

  private ByteArrayInputStream bis = null;
  private DataInputStream dis = null;

  public Iterator<M> iterator() {
    bis = new ByteArrayInputStream(byteBuffer.toByteArray());
    dis = new DataInputStream(bis);

    Iterator<M> it = new Iterator<M>() {
      Class<M> clazz = null;
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
        try {
          if (clazz == null) {
            clazz = (Class<M>) Class.forName(className);
          }

          msg = ReflectionUtils.newInstance(clazz, null);
          boolean isCompressed = false;

          if (compressor != null) {
            isCompressed = dis.readBoolean();
          }

          if (isCompressed) {
            int length = dis.readInt();
            msgBytes = new byte[length];
            dis.readFully(msgBytes);

            mbis = new ByteArrayInputStream(compressor.decompress(msgBytes));
            mdis = new DataInputStream(mbis);
            msg.readFields(mdis);
          } else {
            msg.readFields(dis);
          }

        } catch (IOException ie) {
          LOG.error(ie);
        } catch (ClassNotFoundException ce) {
          LOG.error("Class was not found.", ce);
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

  public void setCompressor(BSPMessageCompressor<M> compressor, long threshold) {
    this.compressor = compressor;
    this.threshold = threshold;
  }

  /**
   * @return the byte length of messages
   * @throws IOException
   */
  public long getLength() {
    return byteBuffer.size();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(bundleSize);
    if (bundleSize > 0) {
      out.writeUTF(className);
      out.writeInt(byteBuffer.size());
      out.write(byteBuffer.toByteArray());
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
      bufferDos.write(temp);
    }
  }

}
