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

import java.io.IOException;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class KryoSerializer {
  public static final int BUFFER_SIZE = 1024;

  private final Kryo kryo;
  private final Class<?> clazz;
  private final byte[] buffer = new byte[BUFFER_SIZE];
  private final Output output = new Output(buffer, -1);
  private final Input input = new Input(buffer);

  public KryoSerializer(Class<?> clazz) {
    kryo = new Kryo();
    kryo.setReferences(false);
    kryo.register(clazz);
    this.clazz = clazz;
  }

  public byte[] serialize(Object obj) throws IOException {
    output.setBuffer(buffer, -1);
    kryo.writeObject(output, obj);
    return output.toBytes();
  }

  public Object deserialize(byte[] bytes) throws IOException {
    input.setBuffer(bytes);
    return kryo.readObject(input, clazz);
  }

}
