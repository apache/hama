/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hama.bsp.sync;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * Custom writable for string arrays, because ArrayWritable has no default
 * constructor and is broken.
 * 
 */
public class StringArrayWritable implements Writable {

  private String[] array;

  public StringArrayWritable() {
    super();
  }

  public StringArrayWritable(String[] array) {
    super();
    this.array = array;
  }

  // no defensive copy needed because this always comes from an rpc call.
  public String[] get() {
    return array;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(array.length);
    for (String s : array) {
      out.writeUTF(s);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    array = new String[in.readInt()];
    for (int i = 0; i < array.length; i++) {
      array[i] = in.readUTF();
    }
  }

}
