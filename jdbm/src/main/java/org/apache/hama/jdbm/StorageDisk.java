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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.OverlappingFileLockException;
import java.util.ArrayList;
import java.util.List;

/**
 * Storage which used files on disk to store data
 */
public final class StorageDisk implements Storage {

  private ArrayList<RandomAccessFile> rafs = new ArrayList<RandomAccessFile>();
  private ArrayList<RandomAccessFile> rafsTranslation = new ArrayList<RandomAccessFile>();

  private String fileName;

  private boolean readonly;

  public StorageDisk(String fileName, boolean readonly, boolean lockingDisabled)
      throws IOException {
    this.fileName = fileName;
    this.readonly = readonly;
    // make sure first file can be opened
    // lock it
    try {
      if (!readonly && !lockingDisabled)
        getRaf(0).getChannel().tryLock();
    } catch (IOException e) {
      throw new IOException("Could not lock DB file: " + fileName, e);
    } catch (OverlappingFileLockException e) {
      throw new IOException("Could not lock DB file: " + fileName, e);
    }

  }

  RandomAccessFile getRaf(long pageNumber) throws IOException {

    int fileNumber = (int) (Math.abs(pageNumber) / StorageDiskMapped.PAGES_PER_FILE);

    List<RandomAccessFile> c = pageNumber >= 0 ? rafs : rafsTranslation;

    // increase capacity of array lists if needed
    for (int i = c.size(); i <= fileNumber; i++) {
      c.add(null);
    }

    RandomAccessFile ret = c.get(fileNumber);
    if (ret == null) {
      String name = StorageDiskMapped.makeFileName(fileName, pageNumber,
          fileNumber);
      ret = new RandomAccessFile(name, readonly ? "r" : "rw");
      c.set(fileNumber, ret);
    }
    return ret;

  }

  @Override
  public void write(long pageNumber, ByteBuffer data) throws IOException {
    if (data.capacity() != PAGE_SIZE)
      throw new IllegalArgumentException();

    long offset = pageNumber * PAGE_SIZE;

    RandomAccessFile file = getRaf(pageNumber);

    file.seek(Math.abs(offset % (StorageDiskMapped.PAGES_PER_FILE * PAGE_SIZE)));

    file.write(data.array());
  }

  @Override
  public ByteBuffer read(long pageNumber) throws IOException {

    long offset = pageNumber * PAGE_SIZE;
    ByteBuffer buffer = ByteBuffer.allocate(PAGE_SIZE);

    RandomAccessFile file = getRaf(pageNumber);
    file.seek(Math.abs(offset % (StorageDiskMapped.PAGES_PER_FILE * PAGE_SIZE)));
    int remaining = buffer.limit();
    int pos = 0;
    while (remaining > 0) {
      int read = file.read(buffer.array(), pos, remaining);
      if (read == -1) {
        System
            .arraycopy(PageFile.CLEAN_DATA, 0, buffer.array(), pos, remaining);
        break;
      }
      remaining -= read;
      pos += read;
    }
    return buffer;
  }

  static final String transaction_log_file_extension = ".t";

  @Override
  public DataOutputStream openTransactionLog() throws IOException {
    String logName = fileName + transaction_log_file_extension;
    final FileOutputStream fileOut = new FileOutputStream(logName);
    return new DataOutputStream(new BufferedOutputStream(fileOut)) {

      // default implementation of flush on FileOutputStream does nothing,
      // so we use little workaround to make sure that data were really flushed
      @Override
      public void flush() throws IOException {
        super.flush();
        fileOut.flush();
        fileOut.getFD().sync();
      }
    };
  }

  @Override
  public void deleteAllFiles() {
    deleteTransactionLog();
    StorageDiskMapped.deleteFiles(fileName);
  }

  /**
   * Synchronizes the file.
   */
  @Override
  public void sync() throws IOException {
    for (RandomAccessFile file : rafs)
      if (file != null)
        file.getFD().sync();
    for (RandomAccessFile file : rafsTranslation)
      if (file != null)
        file.getFD().sync();
  }

  @Override
  public void forceClose() throws IOException {
    for (RandomAccessFile f : rafs) {
      if (f != null)
        f.close();
    }
    rafs = null;
    for (RandomAccessFile f : rafsTranslation) {
      if (f != null)
        f.close();
    }
    rafsTranslation = null;
  }

  @Override
  public DataInputStream readTransactionLog() {

    File logFile = new File(fileName + transaction_log_file_extension);
    if (!logFile.exists())
      return null;
    if (logFile.length() == 0) {
      logFile.delete();
      return null;
    }

    DataInputStream ois = null;
    try {
      ois = new DataInputStream(new BufferedInputStream(new FileInputStream(
          logFile)));
    } catch (FileNotFoundException e) {
      // file should exists, we check for its presents just a miliseconds
      // yearlier, anyway move on
      return null;
    }

    try {
      if (ois.readShort() != Magic.LOGFILE_HEADER)
        throw new Error("Bad magic on log file");
    } catch (IOException e) {
      // corrupted/empty logfile
      logFile.delete();
      return null;
    }
    return ois;
  }

  @Override
  public void deleteTransactionLog() {
    File logFile = new File(fileName + transaction_log_file_extension);
    if (logFile.exists())
      logFile.delete();
  }

  @Override
  public boolean isReadonly() {
    return false;
  }
}
