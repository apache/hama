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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

/**
 * 
 *
 */
public class WriteSpilledDataProcessor implements SpilledDataProcessor {

  private static final Log LOG = LogFactory
      .getLog(WriteSpilledDataProcessor.class);

  private FileChannel fileChannel;
  private RandomAccessFile raf;
  private String fileName;

  public WriteSpilledDataProcessor(String fileName)
      throws FileNotFoundException {
    this.fileName = fileName;
    raf = new RandomAccessFile(fileName, "rw");
    fileChannel = raf.getChannel();
  }

  @Override
  public boolean init(Configuration conf) {
    return true;
  }

  @Override
  public boolean handleSpilledBuffer(ByteBuffer buffer) {
    try {
      fileChannel.write(buffer);
      return true;
    } catch (IOException e) {
      LOG.error("Error writing to file:"+fileName, e);
    }
    return false;
  }

  @Override
  public boolean close() {
    try {
      fileChannel.close();
    } catch (IOException e) {
      LOG.error("Error writing to file:" + fileName, e);
    }
    return true;
  }

}
