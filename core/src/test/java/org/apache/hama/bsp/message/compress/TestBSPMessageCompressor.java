package org.apache.hama.bsp.message.compress;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hama.bsp.BSPMessage;
import org.apache.hama.bsp.BSPMessageBundle;
import org.apache.hama.bsp.IntegerMessage;

public class TestBSPMessageCompressor extends TestCase {

  public void testCompression() {
    BSPMessageCompressor<IntegerMessage> compressor = new BSPMessageCompressorFactory<IntegerMessage>()
        .getCompressor(new Configuration());

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
