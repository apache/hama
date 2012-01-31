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
package org.apache.hama.examples.util;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hama.graph.VertexArrayWritable;
import org.apache.hama.graph.VertexWritable;
import org.apache.hama.util.KeyValuePair;

/**
 * Abstract base class for turning a text graph into a sequence file. It offers
 * help for multiple inputs in a directory.
 */
public abstract class TextToSequenceFile {

  protected static final Log LOG = LogFactory.getLog(TextToSequenceFile.class);

  protected final Path inPath;
  protected final Path outPath;
  protected final String delimiter;
  protected final Configuration conf;
  protected final FileSystem sourceFs;
  protected final FileSystem destFs;

  public TextToSequenceFile(Path inPath, Path outPath, String delimiter)
      throws IOException {
    super();
    this.inPath = inPath;
    this.outPath = outPath;
    this.delimiter = delimiter;

    this.conf = new Configuration();
    this.sourceFs = inPath.getFileSystem(conf);
    this.destFs = outPath.getFileSystem(conf);
  }

  public final void run() throws IOException {
    final FileStatus[] stati = sourceFs.globStatus(inPath);
    if (stati == null || stati.length == 0) {
      throw new FileNotFoundException("Cannot access " + inPath
          + ": No such file or directory.");
    }

    for (int i = 0; i < stati.length; i++) {
      final Path p = stati[i].getPath();
      if (!sourceFs.getFileStatus(p).isDir()) {
        Writer writer = null;
        try {
          LOG.info("Processing file : " + p);
          Path out = new Path(outPath, p.getName() + ".seq");
          writer = getWriter(out);
          processFile(p, writer);
          LOG.info("Written " + writer.getLength() + " bytes to " + out);
        } finally {
          if (writer != null)
            writer.close();
        }
      } else {
        LOG.warn("Skipping dir : " + p);
      }
    }

  }

  private final void processFile(Path p, SequenceFile.Writer writer)
      throws IOException {
    final FileStatus fileStatus = sourceFs.getFileStatus(p);
    if (sourceFs.exists(p) && !fileStatus.isDir()) {
      BufferedReader br = null;
      try {
        br = new BufferedReader(new InputStreamReader(sourceFs.open(p)));
        String line;
        while ((line = br.readLine()) != null) {
          final KeyValuePair<VertexWritable, VertexArrayWritable> kv = processLine(line);
          writer.append(kv.getKey(), kv.getValue());
        }
      } finally {
        if (br != null)
          br.close();
      }
    } else {
      LOG.error(p + " is a directory or does not exist!");
    }
  }

  protected abstract SequenceFile.Writer getWriter(Path outPath)
      throws IOException;

  protected abstract KeyValuePair<VertexWritable, VertexArrayWritable> processLine(
      String line) throws IOException;

}
