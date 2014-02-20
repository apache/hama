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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hama.bsp.message.type.IntegerMessage;

public class TestBSPMessageCompressor extends TestCase {

  public void testCompression() throws IOException {
    Configuration configuration = new Configuration();
    BSPMessageCompressor<IntegerMessage> compressor = new BSPMessageCompressorFactory<IntegerMessage>()
        .getCompressor(configuration);

    assertNull(compressor);
    configuration.setClass(BSPMessageCompressorFactory.COMPRESSION_CODEC_CLASS,
        SnappyCompressor.class, BSPMessageCompressor.class);
    compressor = new BSPMessageCompressorFactory<IntegerMessage>()
        .getCompressor(configuration);

    assertNotNull(compressor);

    IntWritable a = new IntWritable(123);
    IntWritable b = new IntWritable(321);
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(bos);
    a.write(dos);
    b.write(dos);

    byte[] x = bos.toByteArray();

    byte[] compressed = compressor.compress(x);
    byte[] decompressed = compressor.decompress(compressed);

    ByteArrayInputStream bis = new ByteArrayInputStream(decompressed);
    DataInputStream dis = new DataInputStream(bis);

    IntWritable c = new IntWritable();
    c.readFields(dis);
    assertEquals(123, c.get());

    IntWritable d = new IntWritable();
    d.readFields(dis);
    assertEquals(321, d.get());
  }
}
