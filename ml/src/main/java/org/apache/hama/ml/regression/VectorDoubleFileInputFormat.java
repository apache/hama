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
package org.apache.hama.ml.regression;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hama.bsp.*;
import org.apache.hama.ml.math.DenseDoubleVector;
import org.apache.hama.ml.math.DoubleVector;
import org.apache.hama.ml.writable.VectorWritable;

import java.io.IOException;
import java.io.InputStream;

/**
 * A {@link FileInputFormat} for files containing one vector and one double per
 * line
 */
public class VectorDoubleFileInputFormat extends
    FileInputFormat<VectorWritable, DoubleWritable> {

  @Override
  public RecordReader<VectorWritable, DoubleWritable> getRecordReader(
      InputSplit split, BSPJob job) throws IOException {
    return new VectorDoubleRecorderReader(job.getConfiguration(), (FileSplit) split);
  }

  static class VectorDoubleRecorderReader implements
      RecordReader<VectorWritable, DoubleWritable> {

    private static final Log LOG = LogFactory
        .getLog(VectorDoubleRecorderReader.class.getName());

    private CompressionCodecFactory compressionCodecs = null;
    private long start;
    private long pos;
    private long end;
    private LineReader in;
    int maxLineLength;

    /**
     * A class that provides a line reader from an input stream.
     */
    public static class LineReader extends org.apache.hadoop.util.LineReader {
      LineReader(InputStream in) {
        super(in);
      }

      LineReader(InputStream in, int bufferSize) {
        super(in, bufferSize);
      }

      public LineReader(InputStream in, Configuration conf) throws IOException {
        super(in, conf);
      }
    }

    public VectorDoubleRecorderReader(Configuration job, FileSplit split)
        throws IOException {
      this.maxLineLength = job.getInt("bsp.linerecordreader.maxlength",
          Integer.MAX_VALUE);
      start = split.getStart();
      end = start + split.getLength();
      final Path file = split.getPath();
      compressionCodecs = new CompressionCodecFactory(job);
      final CompressionCodec codec = compressionCodecs.getCodec(file);

      // open the file and seek to the start of the split
      FileSystem fs = file.getFileSystem(job);
      FSDataInputStream fileIn = fs.open(split.getPath());
      boolean skipFirstLine = false;
      if (codec != null) {
        in = new LineReader(codec.createInputStream(fileIn), job);
        end = Long.MAX_VALUE;
      } else {
        if (start != 0) {
          skipFirstLine = true;
          --start;
          fileIn.seek(start);
        }
        in = new LineReader(fileIn, job);
      }
      if (skipFirstLine) { // skip first line and re-establish "start".
        start += in.readLine(new Text(), 0,
            (int) Math.min(Integer.MAX_VALUE, end - start));
      }
      this.pos = start;
    }

    public VectorDoubleRecorderReader(InputStream in, long offset,
        long endOffset, int maxLineLength) {
      this.maxLineLength = maxLineLength;
      this.in = new LineReader(in);
      this.start = offset;
      this.pos = offset;
      this.end = endOffset;
    }

    public VectorDoubleRecorderReader(InputStream in, long offset,
        long endOffset, Configuration job) throws IOException {
      this.maxLineLength = job.getInt("bsp.linerecordreader.maxlength",
          Integer.MAX_VALUE);
      this.in = new LineReader(in, job);
      this.start = offset;
      this.pos = offset;
      this.end = endOffset;
    }

    @Override
    public VectorWritable createKey() {
      return new VectorWritable();
    }

    @Override
    public DoubleWritable createValue() {
      return new DoubleWritable();
    }

    /**
     * Read a line.
     */
    @Override
    public synchronized boolean next(VectorWritable key, DoubleWritable value)
        throws IOException {

      while (pos < end) {

        Text textVal = new Text();
        int newSize = in.readLine(textVal, maxLineLength, Math.max(
            (int) Math.min(Integer.MAX_VALUE, end - pos), maxLineLength));
        if (newSize == 0) {
          return false;
        }

        String[] kv = new String(textVal.getBytes()).split("\\>");
        if (kv.length != 2) {
          throw new IOException("a line was not parsed correctly");
        }
        value.set(Double.valueOf(kv[0]));
        key.set(toDoubleVector(kv[1]));

        if (LOG.isDebugEnabled()) {
          LOG.info("reading " + kv[1] + ":" + kv[0]);
        }

        pos += newSize;
        if (newSize < maxLineLength) {
          return true;
        }

        // line too long. try again
        LOG.info("Skipped line of size " + newSize + " at pos "
            + (pos - newSize));
      }

      return false;
    }

    private DoubleVector toDoubleVector(String s) {
      String[] split = s.split(" ");
      double[] dar = new double[split.length];
      for (int i = 0; i < split.length; i++) {
        dar[i] = Double.valueOf(split[i]);
      }
      return new DenseDoubleVector(dar);
    }

    /**
     * Get the progress within the split
     */
    @Override
    public float getProgress() {
      if (start == end) {
        return 0.0f;
      } else {
        return Math.min(1.0f, (pos - start) / (float) (end - start));
      }
    }

    @Override
    public synchronized long getPos() throws IOException {
      return pos;
    }

    @Override
    public synchronized void close() throws IOException {
      if (in != null) {
        in.close();
      }
    }
  }
}
