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
package org.apache.hama.bsp.message.io;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hama.Constants;
import org.apache.hama.bsp.Combiner;
import org.apache.hama.util.ReflectionUtils;

/**
 * This data processor adds a stage in between the spillage of data. Based on
 * the combiner provided it combines the bunch of messages in the byte buffer
 * and then writes them to the file writer using the base class handle method.
 * TODO: Experiment the feature with pipelining design.
 */
public class CombineSpilledDataProcessor<M extends Writable> extends
    WriteSpilledDataProcessor {

  public static Log LOG = LogFactory.getLog(CombineSpilledDataProcessor.class);

  Combiner<M> combiner;
  M writableObject;
  ReusableByteBuffer<M> iterator;
  DirectByteBufferOutputStream combineOutputBuffer;
  ByteBuffer combineBuffer;

  public CombineSpilledDataProcessor(String fileName)
      throws FileNotFoundException {
    super(fileName);
  }

  @Override
  public boolean init(Configuration conf) {
    if (!super.init(conf)) {
      return false;
    }
    String className = conf.get(Constants.COMBINER_CLASS);

    if (className == null)
      return true;
    try {
      combiner = ReflectionUtils.newInstance(className);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    className = conf.get(Constants.MESSAGE_CLASS);

    if (className != null) {
      try {
        writableObject = ReflectionUtils.newInstance(className);
        iterator = new ReusableByteBuffer<M>(writableObject);
      } catch (ClassNotFoundException e) {
        LOG.error("Error combining the records.", e);
        return false;
      }
    }

    combineOutputBuffer = new DirectByteBufferOutputStream();
    combineBuffer = ByteBuffer.allocateDirect(Constants.BUFFER_DEFAULT_SIZE);
    combineOutputBuffer.setBuffer(combineBuffer);
    return true;
  }

  @Override
  public boolean handleSpilledBuffer(SpilledByteBuffer buffer) {

    if (combiner == null || writableObject == null) {
      return super.handleSpilledBuffer(buffer);
    }

    try {
      iterator.set(buffer);
    } catch (IOException e1) {
      LOG.error("Error setting buffer for combining data", e1);
      return false;
    }
    Writable combinedMessage = combiner.combine(iterator);
    try {
      iterator.prepareForNext();
    } catch (IOException e1) {
      LOG.error("Error preparing for next buffer.", e1);
      return false;
    }
    try {
      combinedMessage.write(this.combineOutputBuffer);
    } catch (IOException e) {
      LOG.error("Error writing the combiner output.", e);
      return false;
    }
    try {
      this.combineOutputBuffer.flush();
    } catch (IOException e) {
      LOG.error("Error flushing the combiner output.", e);
      return false;
    }
    this.combineOutputBuffer.getBuffer().flip();
    try {
      return super.handleSpilledBuffer(new SpilledByteBuffer(
          this.combineOutputBuffer.getBuffer(), this.combineOutputBuffer
              .getBuffer().remaining()));
    } finally {
      this.combineOutputBuffer.clear();
    }
  }

  @Override
  public boolean close() {
    return true;
  }

}
