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

import junit.framework.TestCase;

import org.apache.hadoop.io.DoubleWritable;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class TestKryoSerializer extends TestCase {

  public void testSerialization() throws Exception {
    Kryo kryo = new Kryo();
    kryo.register(DoubleWritable.class);

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    Output out = new Output(outputStream, 4096);

    for (int i = 0; i < 10; i++) {
      DoubleWritable a = new DoubleWritable(i + 0.123);
      kryo.writeClassAndObject(out, a);
      out.flush();
    }

    System.out.println(outputStream.size());
    
    ByteArrayInputStream bin = new ByteArrayInputStream(outputStream.toByteArray());
    Input in = new Input(bin, 4096);

    for (int i = 0; i < 10; i++) {
      DoubleWritable b = (DoubleWritable) kryo.readClassAndObject(in);
      System.out.println(bin.available() + ", " + b);
    }
  }

}
