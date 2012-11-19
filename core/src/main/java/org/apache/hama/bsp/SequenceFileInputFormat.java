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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;

public class SequenceFileInputFormat<K, V> extends FileInputFormat<K, V> {

  @Override
  public RecordReader<K, V> getRecordReader(InputSplit split, BSPJob job)
      throws IOException {
    return new SequenceFileRecordReader<K, V>(job.getConfiguration(),
        (FileSplit) split);
  }

  @Override
  protected long getFormatMinSplitSize() {
    return SequenceFile.SYNC_INTERVAL;
  }

  @Override
  protected FileStatus[] listStatus(BSPJob job) throws IOException {

    FileStatus[] files = super.listStatus(job);
    int len = files.length;
    for (int i = 0; i < len; ++i) {
      FileStatus file = files[i];
      if (file.isDir()) { // it's a MapFile
        Path p = file.getPath();
        FileSystem fs = p.getFileSystem(job.getConfiguration());
        // use the data file
        files[i] = fs.getFileStatus(new Path(p, MapFile.DATA_FILE_NAME));
      }
    }
    return files;
  }

}
