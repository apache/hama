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
package org.apache.hama.bsp.message.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A message that consists of a string tag and a boolean value.
 */
public class BooleanMessage extends BSPMessage {

  public String tag;
  public boolean data;

  public BooleanMessage() {
    super();
  }

  public BooleanMessage(String tag, boolean data) {
    super();
    this.tag = tag;
    this.data = data;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(tag);
    out.writeBoolean(data);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    tag = in.readUTF();
    data = in.readBoolean();
  }

  @Override
  public String getTag() {
    return tag;
  }

  @Override
  public Boolean getData() {
    return data;
  }

  @Override
  public void setTag(Object tag) {
    this.tag = (String) tag;
  }

  @Override
  public void setData(Object data) {
    this.data = (Boolean) data;
  }
}
