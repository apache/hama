/**
 * Copyright 2007 The Apache Software Foundation
 *
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Writable;

public class SequenceFileRecordWriter<K extends Writable, V extends Writable>
    implements RecordWriter<K, V> {

  private Writer writer;

  public SequenceFileRecordWriter(FileSystem fs, BSPJob job, String name)
      throws IOException, ClassNotFoundException {
    Configuration conf = job.getConf();
    writer = new SequenceFile.Writer(fs, conf, new Path(
        conf.get("bsp.output.dir"), name), conf.getClassByName(conf
        .get("bsp.output.key.class")), conf.getClassByName(conf
        .get("bsp.output.value.class")));
  }

  @Override 
  public void write(K key, V value) throws IOException {
    writer.append(key, value);
  }

  @Override
  public void close() throws IOException {
    writer.close();
  }

}
