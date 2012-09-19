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

package org.apache.hama.pipes;

import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * The abstract description of the downward (from Java to C++) Pipes protocol.
 * All of these calls are asynchronous and return before the message has been
 * processed.
 * 
 * Adapted from Hadoop Pipes.
 * 
 */
public interface DownwardProtocol<K extends Writable, V extends Writable> {

  /**
   * Start communication
   * 
   * @throws IOException
   */
  void start() throws IOException;

  /**
   * Set the input types for Maps.
   * 
   * @param keyType the name of the key's type
   * @param valueType the name of the value's type
   * @throws IOException
   */
  void setInputTypes(String keyType, String valueType) throws IOException;

  void runBsp(boolean pipedInput, boolean pipedOutput) throws IOException;

  void runCleanup(boolean pipedInput, boolean pipedOutput) throws IOException;

  void runSetup(boolean pipedInput, boolean pipedOutput) throws IOException;

  /**
   * The task should stop as soon as possible, because something has gone wrong.
   * 
   * @throws IOException
   */
  void abort() throws IOException;

  /**
   * Flush the data through any buffers.
   */
  void flush() throws IOException;

  /**
   * Close the connection.
   */
  void close() throws IOException, InterruptedException;

  boolean waitForFinish() throws IOException, InterruptedException;
}
