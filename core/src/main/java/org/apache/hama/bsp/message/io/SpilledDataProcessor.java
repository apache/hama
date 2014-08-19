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

import org.apache.hadoop.conf.Configuration;

/**
 * Base interface defining the behaviour to process the spilled data provided in
 * a byte buffer.
 */
public interface SpilledDataProcessor {

  /**
   * Initialize the data processor.
   * 
   * @param conf
   * @return true if no errors.
   */
  boolean init(Configuration conf);

  /**
   * Override the method to define the action to be taken on the spilled data
   * provided in the byte buffer.
   * 
   * @param buffer
   * @return true if no errors.
   */
  boolean handleSpilledBuffer(SpilledByteBuffer buffer);

  /**
   * Close the data processor.
   * 
   * @return true if no errors.
   */
  boolean close();

}
