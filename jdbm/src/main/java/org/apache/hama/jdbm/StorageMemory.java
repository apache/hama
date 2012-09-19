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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Storage which keeps all data in memory. Data are lost after storage is
 * closed.
 */
public final class StorageMemory implements Storage {

  private LongHashMap<byte[]> pages = new LongHashMap<byte[]>();
  private boolean transactionsDisabled;

  StorageMemory(boolean transactionsDisabled) {
    this.transactionsDisabled = transactionsDisabled;
  }

  @Override
  public ByteBuffer read(long pageNumber) throws IOException {

    byte[] data = pages.get(pageNumber);
    if (data == null) {
      // out of bounds, so just return empty data
      return ByteBuffer.wrap(PageFile.CLEAN_DATA).asReadOnlyBuffer();
    } else {
      ByteBuffer b = ByteBuffer.wrap(data);
      if (!transactionsDisabled)
        return b.asReadOnlyBuffer();
      else
        return b;
    }

  }

  @Override
  public void write(long pageNumber, ByteBuffer data) throws IOException {
    if (data.capacity() != PAGE_SIZE)
      throw new IllegalArgumentException();

    byte[] b = pages.get(pageNumber);

    if (transactionsDisabled && data.hasArray() && data.array() == b) {
      // already putted directly into array
      return;
    }

    if (b == null)
      b = new byte[PAGE_SIZE];

    data.position(0);
    data.get(b, 0, PAGE_SIZE);
    pages.put(pageNumber, b);
  }

  @Override
  public void sync() throws IOException {
  }

  @Override
  public void forceClose() throws IOException {
    pages = null;
  }

  private ByteArrayOutputStream transLog;

  @Override
  public DataInputStream readTransactionLog() {
    if (transLog == null)
      return null;
    DataInputStream ret = new DataInputStream(new ByteArrayInputStream(
        transLog.toByteArray()));
    // read stream header
    try {
      ret.readShort();
    } catch (IOException e) {
      throw new IOError(e);
    }
    return ret;
  }

  @Override
  public void deleteTransactionLog() {
    transLog = null;
  }

  @Override
  public DataOutputStream openTransactionLog() throws IOException {
    if (transLog == null)
      transLog = new ByteArrayOutputStream();
    return new DataOutputStream(transLog);
  }

  @Override
  public void deleteAllFiles() throws IOException {
  }

  @Override
  public boolean isReadonly() {
    return false;
  }
}
