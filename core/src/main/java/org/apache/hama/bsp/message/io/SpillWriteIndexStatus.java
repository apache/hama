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

import java.io.IOException;
import java.util.BitSet;

/**
 * This class provides a thread-safe interface for both the spilling thread and
 * the thread that is writing into {@link SpillingDataOutputBuffer}. It stores
 * the state and manipulates the next available buffer for both the threads.
 */
class SpillWriteIndexStatus {

  private volatile int bufferListWriteIndex;
  private volatile int processorBufferIndex;
  private volatile boolean spillComplete;
  private volatile boolean spillStart;
  private int numBuffers;
  private volatile BitSet bufferBitState;
  private volatile boolean errorState;

  SpillWriteIndexStatus(int size, int bufferCount, int bufferIndex,
      int fileWriteIndex, BitSet bufferBitState) {
    spillComplete = false;
    spillStart = false;
    bufferListWriteIndex = bufferIndex;
    processorBufferIndex = fileWriteIndex;
    numBuffers = bufferCount;
    this.bufferBitState = bufferBitState;
    errorState = false;
  }

  /**
   * Returns the index of next available buffer from the list. The call blocks
   * until it is available and indicated by the spilling thread.
   * 
   * @return the index of the next available buffer
   * @throws IOException when the thread is interrupted while blocked on the
   *           availability of the next buffer.
   */
  public synchronized int getNextBufferIndex() throws IOException {
    assert !spillComplete;
    bufferBitState.set(bufferListWriteIndex, true);
    notify();
    bufferListWriteIndex = (bufferListWriteIndex + 1) % numBuffers;
    while (bufferBitState.get(bufferListWriteIndex) && !errorState) {
      try {
        wait();
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
    }
    return checkError(bufferListWriteIndex);
  }

  private int checkError(int index) {
    return errorState ? -1 : index;
  }

  public void notifyError() {
    errorState = true;
    notify();
  }

  /**
   * Returns the index of the next buffer from the list that has information
   * written to be spilled to disk. The call blocks until a buffer is available
   * and indicated by the data writing thread.
   * 
   * @return the index of the next to be spilled buffer.
   * @throws InterruptedException when the thread is interrupted while blocked
   *           on the availability of the next buffer.
   */
  public synchronized int getNextProcessorBufferIndex()
      throws InterruptedException {

    // Indicate to main thread that the spilling thread has started.
    if (!spillStart) {
      spillStart = true;
      notify();
    }
    if (processorBufferIndex >= 0) {
      assert bufferBitState.get(processorBufferIndex);
      bufferBitState.set(processorBufferIndex, false);
      notify();
    }
    processorBufferIndex = (processorBufferIndex + 1) % numBuffers;
    while (!bufferBitState.get(processorBufferIndex) && !spillComplete
        && !errorState) {
      wait();
    }
    // Is the last buffer written to file after the spilling is complete?
    // then complete the operation.
    if ((spillComplete && bufferBitState.isEmpty()) || errorState) {
      return -1;
    }
    return checkError(processorBufferIndex);
  }

  /**
   * This call indicates that the spilling has started. It is blocked until the
   * spilling thread makes its first call to get the buffer with data to be
   * spilled.
   * 
   * @throws InterruptedException
   * @return whether the spill started correctly
   */
  public synchronized boolean startSpilling() throws InterruptedException {

    while (!spillStart && !errorState) {
      wait();
    }
    return !errorState;
  }

  /**
   * This call informs the closing of the buffer and thereby indicating the
   * spilling thread to complete.
   */
  public synchronized void spillCompleted() {
    assert !spillComplete;
    spillComplete = true;
    bufferBitState.set(bufferListWriteIndex, true);
    notify();
  }

}
