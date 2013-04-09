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

import org.apache.hadoop.io.Text;

/**
 * An {@link InputFormat} for plain text files. Files are broken into lines.
 * Either linefeed or carriage-return are used to signal end of line. Each line
 * is divided into key and value parts by a separator byte. If no such a byte
 * exists, the key will be the entire line and value will be empty.
 */
public class KeyValueTextInputFormat extends FileInputFormat<Text, Text> {

  @Override
  public RecordReader<Text, Text> getRecordReader(InputSplit genericSplit,
      BSPJob job) throws IOException {
    return new KeyValueLineRecordReader(job.conf, (FileSplit) genericSplit);
  }

}
