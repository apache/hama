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

import org.apache.hadoop.io.Writable;

public class BSPMessage implements Writable {
  protected byte[] tag;
  protected byte[] data;

  public BSPMessage() {
  }

  /**
   * Constructor 
   * 
   * @param tag of data
   * @param data of message
   */
  public BSPMessage(byte[] tag, byte[] data) {
    this.tag = new byte[tag.length];
    this.data = new byte[data.length];
    System.arraycopy(tag, 0, this.tag, 0, tag.length);
    System.arraycopy(data, 0, this.data, 0, data.length);
  }

  /**
   * BSP messages are typically identified with tags. This allows to get the tag
   * of data.
   * 
   * @return tag of data of BSP message
   */
  public byte[] getTag() {
    byte[] result = this.tag;
    return result;
  }

  /**
   * @return data of BSP message
   */
  public byte[] getData() {
    byte[] result = this.data;
    return result;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.tag = new byte[in.readInt()];
    in.readFully(tag, 0, this.tag.length);
    this.data = new byte[in.readInt()];
    in.readFully(data, 0, this.data.length);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(tag.length);
    out.write(tag);
    out.writeInt(data.length);
    out.write(data);
  }
}
