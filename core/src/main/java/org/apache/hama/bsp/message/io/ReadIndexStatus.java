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
 * The base class that defines the shared object that synchronizes the indexes
 * for the byte array such that there is no loss of data.
 * 
 */
abstract class ReadIndexStatus {

  /**
   * Returns index of the byte array that is ready for the data to be read from.
   * The implementation may or may not block depending on the spilling
   * situation.
   * 
   * @return the index.
   * @throws InterruptedException
   */
  public abstract int getReadBufferIndex() throws InterruptedException;

  /**
   * Returns the index of the byte array that could used to load the data from
   * the spilled file. The implementation may or may not block depending on the
   * spilling situation.
   * 
   * @return
   * @throws InterruptedException
   */
  public abstract int getFileBufferIndex() throws InterruptedException;

  /**
   * Indicate that the operation is complete.
   */
  public abstract void completeReading();

  /**
   * Indicate to start reading.
   */
  public abstract boolean startReading();

}
