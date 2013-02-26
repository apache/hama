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

/**
 * The implementation of shared object when there is no spilling in the first
 * place.
 * 
 */
class BufferReadStatus extends ReadIndexStatus {

  private int index;
  private int count;

  public BufferReadStatus(int bufferCount) {
    index = -1;
    count = bufferCount;
  }

  @Override
  public int getReadBufferIndex() {
    if (count == index - 1) {
      return -1;
    }
    return ++index;
  }

  @Override
  public int getFileBufferIndex() {
    return -1;
  }

  @Override
  public void completeReading() {
  }

  @Override
  public boolean startReading() {
    return true;
  }

}
