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

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hama.bsp.BSPMessage;
import org.apache.hama.bsp.BSPMessageBundle;
import org.apache.hama.bsp.IntegerMessage;

public class TestBSPMessageCompressor extends TestCase {

  public void testCompression() {
    Configuration configuration = new Configuration();
    BSPMessageCompressor<IntegerMessage> compressor = new BSPMessageCompressorFactory<IntegerMessage>()
        .getCompressor(configuration);

    assertNull(compressor);
    configuration.setClass(BSPMessageCompressorFactory.COMPRESSION_CODEC_CLASS,
        SnappyCompressor.class, BSPMessageCompressor.class);
    compressor = new BSPMessageCompressorFactory<IntegerMessage>()
        .getCompressor(configuration);
    
    assertNotNull(compressor);

    int n = 20;
    BSPMessageBundle<IntegerMessage> bundle = new BSPMessageBundle<IntegerMessage>();
    IntegerMessage[] dmsg = new IntegerMessage[n];

    for (int i = 1; i <= n; i++) {
      dmsg[i - 1] = new IntegerMessage("" + i, i);
      bundle.addMessage(dmsg[i - 1]);
    }

    BSPCompressedBundle compBundle = compressor.compressBundle(bundle);
    BSPMessageBundle<IntegerMessage> uncompBundle = compressor
        .decompressBundle(compBundle);

    int i = 1;
    for (BSPMessage msg : uncompBundle.getMessages()) {
      assertEquals(msg.getData(), i);
      i++;
    }

  }

}
