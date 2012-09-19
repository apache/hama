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
package org.apache.hama.pipes;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.InputFormat;
import org.apache.hama.bsp.InputSplit;
import org.apache.hama.bsp.RecordReader;
import org.apache.hama.bsp.TextInputFormat;

/**
 * Dummy input format used when non-Java a {@link RecordReader} is used by the
 * Pipes' application.
 * 
 * The only useful thing this does is set up the Map-Reduce job to get the
 * {@link PipesDummyRecordReader}, everything else left for the 'actual'
 * InputFormat specified by the user which is given by
 * <i>mapred.pipes.user.inputformat</i>.
 * 
 * Adapted from Hadoop Pipes.
 * 
 */
public class PipesNonJavaInputFormat implements
    InputFormat<FloatWritable, NullWritable> {

  @Override
  public RecordReader<FloatWritable, NullWritable> getRecordReader(
      InputSplit genericSplit, BSPJob job) throws IOException {
    return new PipesDummyRecordReader(job.getConf(), genericSplit);
  }

  @Override
  public InputSplit[] getSplits(BSPJob job, int numSplits) throws IOException {
    // Delegate the generation of input splits to the 'original' InputFormat
    return ReflectionUtils.newInstance(
        job.getConf().getClass("hama.pipes.user.inputformat",
            TextInputFormat.class, InputFormat.class), job.getConf())
        .getSplits(job, numSplits);
  }

  /**
   * A dummy {@link org.apache.hadoop.mapred.RecordReader} to help track the
   * progress of Hama Pipes' applications when they are using a non-Java
   * <code>RecordReader</code>.
   * 
   * The <code>PipesDummyRecordReader</code> is informed of the 'progress' of
   * the task by the {@link OutputHandler#progress(float)} which calls the
   * {@link #next(FloatWritable, NullWritable)} with the progress as the
   * <code>key</code>.
   */
  static class PipesDummyRecordReader implements
      RecordReader<FloatWritable, NullWritable> {
    float progress = 0.0f;

    public PipesDummyRecordReader(Configuration job, InputSplit split)
        throws IOException {
    }

    @Override
    public FloatWritable createKey() {
      return null;
    }

    @Override
    public NullWritable createValue() {
      return null;
    }

    @Override
    public synchronized void close() throws IOException {
    }

    @Override
    public synchronized long getPos() throws IOException {
      return 0;
    }

    @Override
    public float getProgress() {
      return progress;
    }

    @Override
    public synchronized boolean next(FloatWritable key, NullWritable value)
        throws IOException {
      progress = key.get();
      return true;
    }
  }
}
