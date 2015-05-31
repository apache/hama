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
package org.apache.hama.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class WritableUtils {

  public static byte[] serialize(Writable w) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DataOutput output = new DataOutputStream(out);
    try {
      w.write(output);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return out.toByteArray();
  }

  public static void deserialize(byte[] bytes, Writable obj) {
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));
    try {
      obj.readFields(in);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static byte[] unsafeSerialize(Writable w) {
    UnsafeByteArrayOutputStream out = new UnsafeByteArrayOutputStream();
    DataOutput output = new DataOutputStream(out);
    try {
      w.write(output);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return out.toByteArray();
  }

  public static void unsafeDeserialize(byte[] bytes, Writable obj) {
    DataInputStream in = new DataInputStream(new UnsafeByteArrayInputStream(bytes));
    try {
      obj.readFields(in);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
