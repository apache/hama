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

import java.util.BitSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * The implementation of the shared object when the buffer has already spilled
 * to disk.
 * 
 */
class SpilledDataReadStatus extends ReadIndexStatus {

  private static final Log LOG = LogFactory.getLog(SpilledDataReadStatus.class);

  private volatile int readBufferIndex_;
  private volatile int fetchFileBufferIndex_;
  private int totalSize_;
  private volatile boolean spilledReadStart_;
  private volatile boolean fileReadComplete_;
  private volatile boolean bufferReadComplete_;
  private volatile BitSet bufferBitState_;
  private volatile boolean errorState_;

  public SpilledDataReadStatus(int totalSize, BitSet bufferBitState) {
    readBufferIndex_ = -1;
    fetchFileBufferIndex_ = 0;
    spilledReadStart_ = false;
    fileReadComplete_ = false;
    bufferReadComplete_ = false;
    totalSize_ = totalSize;
    bufferBitState_ = bufferBitState;
    errorState_ = false;
  }

  private int checkError(int index) {
    if (errorState_) {
      return -1;
    } else {
      return index;
    }
  }

  public void notifyError() {
    errorState_ = true;
    notify();
  }

  @Override
  public synchronized int getReadBufferIndex() throws InterruptedException {

    assert !bufferReadComplete_;

    if (readBufferIndex_ >= 0) {
      bufferBitState_.set(readBufferIndex_, false);
      notify();
    }
    readBufferIndex_ = (readBufferIndex_ + 1) % totalSize_;
    while (!bufferBitState_.get(readBufferIndex_) && !fileReadComplete_
        && !errorState_) {
      wait();
    }
    // The file is completely read and transferred to buffers already.
    if (bufferBitState_.isEmpty() && fileReadComplete_) {
      return -1;
    }
    return checkError(readBufferIndex_);
  }

  @Override
  public synchronized int getFileBufferIndex() throws InterruptedException {

    assert !fileReadComplete_;

    if (bufferReadComplete_) {
      return -1;
    }

    // First read
    if (!spilledReadStart_) {
      fetchFileBufferIndex_ = 0;
      spilledReadStart_ = true;
      notify();
      return fetchFileBufferIndex_;
    }

    bufferBitState_.set(fetchFileBufferIndex_, true);
    notify();
    fetchFileBufferIndex_ = (fetchFileBufferIndex_ + 1) % totalSize_;

    while (bufferBitState_.get(fetchFileBufferIndex_) && !bufferReadComplete_
        && !errorState_) {
      wait();
    }

    if (bufferReadComplete_) {
      return -1;
    }

    return checkError(fetchFileBufferIndex_);
  }

  @Override
  public synchronized void completeReading() {
    bufferReadComplete_ = true;
    if (fileReadComplete_)
      return;
    notify();
  }

  /**
   * Called by the thread to indicate that all the spilled data is completely
   * read.
   */
  public synchronized void closedBySpiller() {
    fileReadComplete_ = true;
    bufferBitState_.set(fetchFileBufferIndex_, true);
    notify();
  }

  @Override
  public synchronized boolean startReading() {
    while (!spilledReadStart_ && !errorState_) {
      try {
        wait();
      } catch (InterruptedException e) {
        LOG.error("Interrupted waiting to read the spilled file.", e);
        throw new RuntimeException(e);
      }
    }
    return !errorState_;
  }

}
