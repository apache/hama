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
package org.apache.hama.jdbm;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public interface Storage {

  /**
   * Bite shift used to calculate page size. If you want to modify page size, do
   * it here.
   * 
   * 1<<9 = 512 1<<10 = 1024 1<<11 = 2048 1<<12 = 4096
   */
  int PAGE_SIZE_SHIFT = 12;

  /**
   * the lenght of single page.
   * <p>
   * !!! DO NOT MODIFY THI DIRECTLY !!!
   */
  int PAGE_SIZE = 1 << PAGE_SIZE_SHIFT;

  /**
   * use 'val & OFFSET_MASK' to quickly get offset within the page;
   */
  long OFFSET_MASK = 0xFFFFFFFFFFFFFFFFL >>> (64 - Storage.PAGE_SIZE_SHIFT);

  void write(long pageNumber, ByteBuffer data) throws IOException;

  ByteBuffer read(long pageNumber) throws IOException;

  void forceClose() throws IOException;

  boolean isReadonly();

  DataInputStream readTransactionLog();

  void deleteTransactionLog();

  void sync() throws IOException;

  DataOutputStream openTransactionLog() throws IOException;

  void deleteAllFiles() throws IOException;
}
