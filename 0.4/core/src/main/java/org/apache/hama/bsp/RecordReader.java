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
package org.apache.hama.bsp;

import java.io.IOException;

public interface RecordReader<K, V> {
  
  /**
   * Reads the next key/value pair from the input for processing.
   * 
   * @param key the key to read data into
   * @param value the value to read data into
   * @return true iff a key/value was read, false if at EOF
   */
  boolean next(K key, V value) throws IOException;

  /**
   * Create an object of the appropriate type to be used as a key.
   * 
   * @return a new key object.
   */
  K createKey();

  /**
   * Create an object of the appropriate type to be used as a value.
   * 
   * @return a new value object.
   */
  V createValue();

  /**
   * Returns the current position in the input.
   * 
   * @return the current position in the input.
   * @throws IOException
   */
  long getPos() throws IOException;

  /**
   * Close this {@link InputSplit} to future operations.
   * 
   * @throws IOException
   */
  public void close() throws IOException;

  /**
   * How much of the input has the {@link RecordReader} consumed i.e. has been
   * processed by?
   * 
   * @return progress from <code>0.0</code> to <code>1.0</code>.
   * @throws IOException
   */
  float getProgress() throws IOException;

}
