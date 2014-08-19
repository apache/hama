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
package org.apache.hama.bsp.message;

import java.io.EOFException;
import java.io.File;
import java.io.RandomAccessFile;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.security.SecureRandom;
import java.util.Iterator;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hama.Constants;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.Combiner;
import org.apache.hama.bsp.message.io.CombineSpilledDataProcessor;
import org.apache.hama.bsp.message.io.DirectByteBufferInputStream;
import org.apache.hama.bsp.message.io.DirectByteBufferOutputStream;
import org.apache.hama.bsp.message.io.ReusableByteBuffer;
import org.apache.hama.bsp.message.io.SpilledByteBuffer;
import org.apache.hama.bsp.message.io.SpilledDataInputBuffer;
import org.apache.hama.bsp.message.io.SpilledDataProcessor;
import org.apache.hama.bsp.message.io.SpillingDataOutputBuffer;
import org.apache.hama.bsp.message.io.SyncFlushByteBufferOutputStream;
import org.apache.hama.bsp.message.io.SyncReadByteBufferInputStream;
import org.apache.hama.bsp.message.io.WriteSpilledDataProcessor;

public class TestMessageIO extends TestCase {

  public void testNonSpillBuffer() throws Exception {

    SpillingDataOutputBuffer outputBuffer = new SpillingDataOutputBuffer();
    Text text = new Text("Testing the spillage of spilling buffer");

    for (int i = 0; i < 100; ++i) {
      text.write(outputBuffer);
    }
    assertTrue(outputBuffer != null);
    assertTrue(outputBuffer.size() == 4000);
    assertFalse(outputBuffer.hasSpilled());
    outputBuffer.close();
  }

  public void testSpillBuffer() throws Exception {

    Configuration conf = new HamaConfiguration();
    String fileName = System.getProperty("java.io.tmpdir") + File.separatorChar
        + new BigInteger(128, new SecureRandom()).toString(32);
    SpilledDataProcessor processor = new WriteSpilledDataProcessor(fileName);
    processor.init(conf);
    SpillingDataOutputBuffer outputBuffer = new SpillingDataOutputBuffer(2,
        1024, 1024, true, processor);
    Text text = new Text("Testing the spillage of spilling buffer");
    for (int i = 0; i < 100; ++i) {
      text.write(outputBuffer);
    }

    assertTrue(outputBuffer != null);
    assertTrue(outputBuffer.size() == 4000);
    assertTrue(outputBuffer.hasSpilled());
    File f = new File(fileName);
    assertTrue(f.exists());
    assertTrue(f.delete());
    outputBuffer.close();

  }

  public static class SumCombiner extends Combiner<IntWritable> {

    @Override
    public IntWritable combine(Iterable<IntWritable> messages) {
      int sum = 0;
      for (IntWritable intObj : messages) {
        sum += intObj.get();
      }
      return new IntWritable(sum);
    }

  }

  public void testSpillingByteBuffer() throws Exception {
    ByteBuffer buffer = ByteBuffer.allocateDirect(512);
    SpilledByteBuffer spillBuffer = new SpilledByteBuffer(buffer);
    for (int i = 0; i < 100; ++i) {
      spillBuffer.putInt(i);
      spillBuffer.markEndOfRecord();
    }
    spillBuffer.putInt(100);
    assertEquals(spillBuffer.getMarkofLastRecord(), 400);
    assertEquals(spillBuffer.remaining(), (512 - 404));
    spillBuffer.flip();
    assertEquals(spillBuffer.remaining(), 404);
    assertEquals(spillBuffer.getMarkofLastRecord(), 400);

  }

  public void testDirectByteBufferOutput() throws Exception {

    ByteBuffer buffer = ByteBuffer.allocateDirect(512);
    DirectByteBufferOutputStream stream = new DirectByteBufferOutputStream();
    stream.setBuffer(buffer);
    IntWritable intWritable = new IntWritable(1);

    for (int i = 0; i < 100; ++i) {
      intWritable.set(i);
      intWritable.write(stream);
    }

    stream.close();

    buffer.flip();
    for (int i = 0; i < 100; ++i) {
      assertEquals(i, buffer.getInt());
    }

    try {
      buffer.getInt();
      assertTrue(false);
    } catch (Exception e) {
      assertTrue(true);
    }

  }

  public void testDirectByteBufferInput() throws Exception {
    ByteBuffer buffer = ByteBuffer.allocateDirect(512);
    DirectByteBufferOutputStream stream = new DirectByteBufferOutputStream();
    stream.setBuffer(buffer);
    IntWritable intWritable = new IntWritable(1);

    for (int i = 0; i < 100; ++i) {
      intWritable.set(i);
      intWritable.write(stream);
    }
    intWritable.write(stream);

    stream.close();

    buffer.flip();

    DirectByteBufferInputStream inStream = new DirectByteBufferInputStream();

    inStream.setBuffer(new SpilledByteBuffer(buffer, 400));
    for (int i = 0; i < 100; ++i) {
      intWritable.readFields(inStream);
      assertEquals(i, intWritable.get());
    }

    assertFalse(inStream.hasDataToRead());
    assertTrue(inStream.hasUnmarkData());
    inStream.prepareForNext();

    // push in another buffer and check if the unmarked data could be read.

    buffer.clear();
    stream = new DirectByteBufferOutputStream();
    buffer = ByteBuffer.allocateDirect(2048);
    stream.setBuffer(buffer);

    for (int i = 0; i < 400; ++i) {
      intWritable.set(i);
      intWritable.write(stream);
    }
    stream.close();
    buffer.flip();

    inStream.setBuffer(new SpilledByteBuffer(buffer, 400));

    // Read previous data
    intWritable.readFields(inStream);
    assertEquals(99, intWritable.get());

    for (int i = 0; i < 100; ++i) {
      intWritable.readFields(inStream);
      assertEquals(i, intWritable.get());
    }

    assertFalse(inStream.hasDataToRead());
    assertTrue(inStream.hasUnmarkData());
    inStream.prepareForNext();

    buffer.clear();
    stream = new DirectByteBufferOutputStream();
    stream.setBuffer(buffer);

    for (int i = 0; i < 100; ++i) {
      intWritable.set(i);
      intWritable.write(stream);
    }
    stream.close();
    buffer.flip();

    inStream.setBuffer(new SpilledByteBuffer(buffer, 400));

    // Read previous data with resized intermediate buffer
    for (int i = 100; i < 400; ++i) {
      intWritable.readFields(inStream);
      assertEquals(i, intWritable.get());
    }

    for (int i = 0; i < 100; ++i) {
      intWritable.readFields(inStream);
      assertEquals(i, intWritable.get());
    }

    assertFalse(inStream.hasDataToRead());
    assertFalse(inStream.hasUnmarkData());

  }

  /**
   * 
   * @throws Exception
   */
  public void testReusableByteBufferIter() throws Exception {

    ReusableByteBuffer<IntWritable> reuseByteBuffer = new ReusableByteBuffer<IntWritable>(
        new IntWritable());

    ByteBuffer buffer = ByteBuffer.allocateDirect(512);
    DirectByteBufferOutputStream stream = new DirectByteBufferOutputStream();
    stream.setBuffer(buffer);
    IntWritable intWritable = new IntWritable(1);

    for (int i = 0; i < 100; ++i) {
      intWritable.set(i);
      intWritable.write(stream);
    }
    intWritable.write(stream);
    stream.close();
    buffer.flip();
    reuseByteBuffer.set(new SpilledByteBuffer(buffer, 400));

    Iterator<IntWritable> iter = reuseByteBuffer.iterator();
    int j = 0;
    while (iter.hasNext()) {
      assertEquals(iter.next().get(), j++);
    }
    assertEquals(j, 100);
    reuseByteBuffer.prepareForNext();

    buffer.clear();

    stream = new DirectByteBufferOutputStream();
    stream.setBuffer(buffer);

    for (int i = 0; i < 101; ++i) {
      intWritable.set(i);
      intWritable.write(stream);
    }
    stream.close();
    buffer.flip();

    reuseByteBuffer.set(new SpilledByteBuffer(buffer, 404));
    iter = reuseByteBuffer.iterator();
    assertEquals(iter.next().get(), 99);

    j = 0;
    while (iter.hasNext()) {
      assertEquals(iter.next().get(), j++);
    }
    buffer.clear();
  }

  public void testCombineProcessor() throws Exception {
    String fileName = System.getProperty("java.io.tmpdir") + File.separatorChar
        + new BigInteger(128, new SecureRandom()).toString(32);

    ByteBuffer buffer = ByteBuffer.allocateDirect(512);
    DirectByteBufferOutputStream stream = new DirectByteBufferOutputStream();
    stream.setBuffer(buffer);
    IntWritable intWritable = new IntWritable(1);
    int sum = 0;
    for (int i = 0; i < 100; ++i) {
      intWritable.set(i);
      intWritable.write(stream);
      sum += i;
    }
    intWritable.write(stream);
    stream.close();
    buffer.flip();

    Configuration conf = new HamaConfiguration();

    conf.setClass(Constants.MESSAGE_CLASS, IntWritable.class, Writable.class);
    conf.setClass(Constants.COMBINER_CLASS, SumCombiner.class, Combiner.class);

    CombineSpilledDataProcessor<IntWritable> processor = new CombineSpilledDataProcessor<IntWritable>(
        fileName);
    assertTrue(processor.init(conf));
    File f = new File(fileName);
    try {
      assertTrue(processor.handleSpilledBuffer(new SpilledByteBuffer(buffer,
          400)));
      buffer.flip();
      assertTrue(processor.handleSpilledBuffer(new SpilledByteBuffer(buffer,
          400)));
      assertTrue(processor.close());

      assertTrue(f.exists());
      assertEquals(f.length(), 8);

      RandomAccessFile raf = new RandomAccessFile(fileName, "r");
      FileChannel fileChannel = raf.getChannel();
      ByteBuffer readBuff = ByteBuffer.allocateDirect(16);
      fileChannel.read(readBuff);
      readBuff.flip();
      assertEquals(readBuff.getInt(), sum);
      assertEquals(readBuff.getInt(), sum + 99);
      raf.close();
    } finally {
      assertTrue(f.delete());
    }

  }

  public void testSpillInputStream() throws Exception {

    File f = null;
    try {
      String fileName = System.getProperty("java.io.tmpdir")
          + File.separatorChar + "testSpillInputStream.txt";
      Configuration conf = new HamaConfiguration();
      SpilledDataProcessor processor = new WriteSpilledDataProcessor(fileName);
      processor.init(conf);
      SpillingDataOutputBuffer outputBuffer = new SpillingDataOutputBuffer(2,
          1024, 1024, true, processor);
      Text text = new Text("Testing the spillage of spilling buffer");
      for (int i = 0; i < 100; ++i) {
        text.write(outputBuffer);
        outputBuffer.markRecordEnd();
      }

      assertTrue(outputBuffer != null);
      assertTrue(outputBuffer.size() == 4000);
      assertTrue(outputBuffer.hasSpilled());
      f = new File(fileName);
      assertTrue(f.exists());
      outputBuffer.close();
      assertTrue(f.length() == 4000);// + (4000 / 1024 + 1) * 4));

      SpilledDataInputBuffer inputBuffer = outputBuffer
          .getInputStreamToRead(fileName);

      for (int i = 0; i < 100; ++i) {
        text.readFields(inputBuffer);
        assertTrue("Testing the spillage of spilling buffer".equals(text
            .toString()));
        text.clear();
      }

      try {
        text.readFields(inputBuffer);
        assertTrue(false);
      } catch (EOFException eof) {
        assertTrue(true);
      }

      inputBuffer.close();
      inputBuffer.completeReading(false);
      assertTrue(f.exists());
      inputBuffer.completeReading(true);
      assertFalse(f.exists());
    } finally {
      if (f != null) {
        if (f.exists()) {
          f.delete();
        }
      }
    }

  }

  public void testSyncFlushByteBufferOutputStream() throws Exception {

    File f = null;
    try {
      String fileName = System.getProperty("java.io.tmpdir")
          + File.separatorChar + "testSyncFlushByteBufferOutputStream.txt";
      SyncFlushByteBufferOutputStream stream = new SyncFlushByteBufferOutputStream(
          fileName);
      DirectByteBufferOutputStream syncFlushStream = new DirectByteBufferOutputStream(
          stream);
      ByteBuffer buffer = ByteBuffer.allocateDirect(512);
      syncFlushStream.setBuffer(buffer);
      IntWritable intWritable = new IntWritable(1);

      for (int i = 0; i < 200; ++i) {
        intWritable.set(i);
        intWritable.write(syncFlushStream);
      }
      intWritable.write(syncFlushStream);
      syncFlushStream.close();

      f = new File(fileName);
      assertTrue(f.exists());
      assertTrue(f.length() == 804);
      assertTrue(f.delete());
    } finally {
      if (f != null) {
        f.delete();
      }
    }

  }

  public void testSyncFlushBufferInputStream() throws Exception {
    File f = null;
    try {
      String fileName = System.getProperty("java.io.tmpdir")
          + File.separatorChar + "testSyncFlushBufferInputStream.txt";
      SyncFlushByteBufferOutputStream stream = new SyncFlushByteBufferOutputStream(
          fileName);
      DirectByteBufferOutputStream syncFlushStream = new DirectByteBufferOutputStream(
          stream);
      ByteBuffer buffer = ByteBuffer.allocateDirect(512);
      syncFlushStream.setBuffer(buffer);
      IntWritable intWritable = new IntWritable(1);

      for (int i = 0; i < 200; ++i) {
        intWritable.set(i);
        intWritable.write(syncFlushStream);
      }
      intWritable.write(syncFlushStream);
      syncFlushStream.close();

      f = new File(fileName);
      assertTrue(f.exists());
      assertEquals(f.length(), 804);

      SyncReadByteBufferInputStream syncReadStream = new SyncReadByteBufferInputStream(
          stream.isSpilled(), fileName);
      DirectByteBufferInputStream inStream = new DirectByteBufferInputStream(
          syncReadStream);
      buffer.clear();
      inStream.setBuffer(buffer);

      for (int i = 0; i < 200; ++i) {
        intWritable.readFields(inStream);
        assertEquals(intWritable.get(), i);
      }

      intWritable.readFields(inStream);
      assertEquals(intWritable.get(), 199);

      try {
        intWritable.readFields(inStream);
        assertFalse(true);
      } catch (Exception e) {
        assertTrue(true);
      }

      inStream.close();
      syncFlushStream.close();

    } finally {
      if (f != null) {
        f.delete();
      }
    }
  }

}
