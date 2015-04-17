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
import java.util.Iterator;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hama.bsp.BSPMessageBundle;

public class TestBSPMessageCompressor extends TestCase {

  public void testCompression() throws IOException {
    Configuration configuration = new Configuration();
    BSPMessageCompressor<IntWritable> compressor = new BSPMessageCompressorFactory<IntWritable>()
        .getCompressor(configuration);

    assertNull(compressor);
    configuration.setClass(BSPMessageCompressorFactory.COMPRESSION_CODEC_CLASS,
        Bzip2Compressor.class, BSPMessageCompressor.class);
    compressor = new BSPMessageCompressorFactory<IntWritable>()
        .getCompressor(configuration);

    assertNotNull(compressor);

    BSPMessageBundle<IntWritable> a = new BSPMessageBundle<IntWritable>();
    for (int i = 0; i < 10000; i++) {
      a.addMessage(new IntWritable(i));
    }

    ByteArrayOutputStream byteBuffer = new ByteArrayOutputStream();
    DataOutputStream bufferDos = new DataOutputStream(byteBuffer);
    a.write(bufferDos);

    byte[] compressed = compressor.compress(byteBuffer.toByteArray());
    assertTrue(byteBuffer.size() > compressed.length);
    byte[] decompressed = compressor.decompress(compressed);

    ByteArrayInputStream bis = new ByteArrayInputStream(decompressed);
    DataInputStream in = new DataInputStream(bis);

    BSPMessageBundle<IntWritable> b = new BSPMessageBundle<IntWritable>();
    b.readFields(in);
    Iterator<IntWritable> it = b.iterator();
    int counter = 0;
    while (it.hasNext()) {
      assertTrue(it.next().get() == counter);
      counter++;
    }
  }
}
