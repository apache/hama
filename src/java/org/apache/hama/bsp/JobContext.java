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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hama.graph.InputFormat;
import org.apache.hama.graph.OutputFormat;

/**
 * A read-only view of the job that is provided to the tasks while they are
 * running.
 */
public class JobContext {
  // Put all of the attribute names in here so that Job and JobContext are
  // consistent.
  protected static final String INPUT_FORMAT_CLASS_ATTR = "angrapa.inputformat.class";
  protected static final String WALKER_CLASS_ATTR = "angrapa.walker.class";
  protected static final String OUTPUT_FORMAT_CLASS_ATTR = "angrapa.outputformat.class";

  protected final Configuration conf;
  private final JobID jobId;

  public JobContext(Configuration conf, JobID jobId) {
    this.conf = conf;
    this.jobId = jobId;
  }

  public Configuration getConfiguration() {
    return conf;
  }

  public JobID getJobID() {
    return jobId;
  }

  public Path getWorkingDirectory() throws IOException {
    String name = conf.get("angrapa.working.dir");

    if (name != null) {
      return new Path(name);
    } else {
      try {
        Path dir = FileSystem.get(conf).getWorkingDirectory();
        conf.set("angrapa.working.dir", dir.toString());
        return dir;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public Class<?> getOutputKeyClass() {
    return conf.getClass("angrapa.output.key.class", LongWritable.class,
        Object.class);
  }

  public Class<?> getOutputValueClass() {
    return conf
        .getClass("angrapa.output.value.class", Text.class, Object.class);
  }

  public String getJobName() {
    return conf.get("angrapa.job.name", "");
  }

  @SuppressWarnings("unchecked")
  public Class<? extends InputFormat<?, ?>> getInputFormatClass()
      throws ClassNotFoundException {
    return (Class<? extends InputFormat<?, ?>>) conf.getClass(
        INPUT_FORMAT_CLASS_ATTR, InputFormat.class); // TODO: To be corrected
    // to an implemented class
  }

  @SuppressWarnings("unchecked")
  public Class<? extends OutputFormat<?, ?>> getOutputFormatClass()
      throws ClassNotFoundException {
    return (Class<? extends OutputFormat<?, ?>>) conf.getClass(
        OUTPUT_FORMAT_CLASS_ATTR, OutputFormat.class); // TODO: To be corrected
    // to an implemented
    // class
  }

  public RawComparator<?> getSortComparator() {
    return null;
  }

  public String getJar() {
    return conf.get("walker.jar");
  }
}
