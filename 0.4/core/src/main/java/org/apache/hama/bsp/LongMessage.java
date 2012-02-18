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

/**
 * A message that consists of a string tag and a long data. 
 */
public class LongMessage extends BSPMessage {

  private String tag;
  private long data;

  public LongMessage() {
    super();
  }

  public LongMessage(String tag, long data) {
    super();
    this.data = data;
    this.tag = tag;
  }

  @Override
  public String getTag() {
    return tag;
  }

  @Override
  public Long getData() {
    return data;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(tag);
    out.writeLong(data);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    tag = in.readUTF();
    data = in.readLong();
  }

}
