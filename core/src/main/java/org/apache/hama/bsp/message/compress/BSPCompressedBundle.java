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
package org.apache.hama.bsp.message.compress;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * A compressed representation of BSPMessageBundle.
 * 
 */
public final class BSPCompressedBundle implements Writable{

  private byte[] data;

  public BSPCompressedBundle(){		
  }

  public BSPCompressedBundle(byte[] data){
    this.data = data;
  }

  public byte[] getData() {
    return data;
  }

  public void setData(byte[] data) {
    this.data = data;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(data.length);
    out.write(data);
  }

  @Override
  public void readFields(DataInput in) throws IOException {		
    int len = in.readInt();
    data    = new byte[len];
    in.readFully(data);
  }

}
