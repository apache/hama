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
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hama.HamaConfiguration;

/**
 * A read-only view of the bsp job that is provided to the tasks while they are
 * running.
 */
public class BSPJobContext {
  // Put all of the attribute names in here so that BSPJob and JobContext are
  // consistent.  
  protected static final String WORK_CLASS_ATTR = "bsp.work.class";
  protected static final String COMBINER_CLASS_ATTR = "bsp.combiner.class";
  protected static final String INPUT_FORMAT_CLASS_ATTR = "bsp.inputformat.class";
  protected static final String OUTPUT_FORMAT_CLASS_ATTR = "bsp.outputformat.class";
  protected static final String WORKING_DIR = "bsp.working.dir";
  
  protected final Configuration conf;
  private final BSPJobID jobId;

  public BSPJobContext(Configuration conf, BSPJobID jobId) {
    this.conf = conf;
    this.jobId = jobId;
  }
  
  public BSPJobContext(Path config, BSPJobID jobId) throws IOException {
    this.conf = new HamaConfiguration();
    this.jobId = jobId;
    this.conf.addResource(config);
  }

  public BSPJobID getJobID() {
    return jobId;
  }

  public Path getWorkingDirectory() throws IOException {
    String name = conf.get(WORKING_DIR);

    if (name != null) {
      return new Path(name);
    } else {
      try {
        Path dir = FileSystem.get(conf).getWorkingDirectory();
        conf.set(WORKING_DIR, dir.toString());
        return dir;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public String getJobName() {
    return conf.get("bsp.job.name", "");
  }

  public String getJar() {
    return conf.get("bsp.jar");
  }
  
  /** 
   * Constructs a local file name. Files are distributed among configured
   * local directories.
   */
  public Path getLocalPath(String pathString) throws IOException {
    return conf.getLocalPath("bsp.local.dir", pathString);
  }
  
  public String getUser() {
    return conf.get("user.name");
  }
  
  public void writeXml(OutputStream out) throws IOException {
    conf.writeXml(out);
  }
  
  public Configuration getConf() {
    return this.conf;
  }
  
  public String get(String name) {
    return conf.get(name);
  }
  
  public int getInt(String name, int defaultValue) {
    return conf.getInt(name, defaultValue);    
  }
}
