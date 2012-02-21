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
package org.apache.hama.bsp.message.compress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;
import org.apache.hama.bsp.BSPMessageBundle;

/**
 * Provides utilities for compressing and decompressing BSPMessageBundle.
 * 
 */
public interface BSPMessageCompressor<M extends Writable> {

  public static final Log LOG = LogFactory.getLog(BSPMessageCompressor.class);

  /**
   * Compresses the BSPMessageBundle and returns it in form of a
   * BSPCompressedBundle.
   * 
   * @param bundle
   * @return
   */
  public BSPCompressedBundle compressBundle(BSPMessageBundle<M> bundle);

  /**
   * Decompresses a BSPCompressedBundle and returns the corresponding
   * BSPMessageBundle.
   * 
   * @param compMsgBundle
   * @return
   */
  public BSPMessageBundle<M> decompressBundle(BSPCompressedBundle compMsgBundle);
}
