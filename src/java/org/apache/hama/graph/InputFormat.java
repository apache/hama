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

package org.apache.hama.graph;

import java.io.IOException;

import java.util.List;

import org.apache.hadoop.mapreduce.InputSplit;

public abstract class InputFormat<KEYIN, VALUEIN> {

  /**
   * Logically split the set of input files for the job.
   * 
   * <p>
   * Each {@link InputSplit} is then assigned to an individual {@link Walker}
   * for processing.
   * </p>
   * 
   * <p>
   * <i>Note</i>: The split is a <i>logical</i> split of the inputs and the
   * input files are not physically split into chunks. For e.g. a split could be
   * <i>&lt;input-file-path, start, offset&gt;</i> tuple. The InputFormat also
   * creates the {@link RecordReader} to read the {@link InputSplit}.
   * 
   * @param context job configuration.
   * @return an array of {@link InputSplit}s for the job.
   */
  public abstract List<InputSplit> getSplits(JobContext context)
      throws IOException, InterruptedException;

  /**
   * Create a record reader for a given split. The framework will call
   * {@link RecordReader#initialize(InputSplit)} before the split is used.
   * 
   * @param split the split to be read
   * @return a new record reader
   * @throws IOException
   * @throws InterruptedException
   */
  public abstract RecordReader<KEYIN, VALUEIN> createRecordReader(
      InputSplit split) throws IOException, InterruptedException;

}
