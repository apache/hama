package org.apache.hama.bsp;

import junit.framework.TestCase;

import org.apache.hama.util.Bytes;

public class TestMessages extends TestCase {

  public void testByteMessage() {
    int dataSize = (int) (Runtime.getRuntime().maxMemory() * 0.60);
    ByteMessage msg = new ByteMessage(Bytes.toBytes("tag"), new byte[dataSize]);
    assertEquals(msg.getData().length, dataSize);
    msg = null;
    
    byte[] dummyData = new byte[1024];
    ByteMessage msg2 = new ByteMessage(Bytes.tail(dummyData, 128), dummyData);
    assertEquals(
        Bytes.compareTo(msg2.getTag(), 0, 128, msg2.getData(),
            msg2.getData().length - 128, 128), 0);
  }
}
