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
package org.apache.hama.util;

import java.io.IOException;

import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hama.Matrix;

/**
 * A map/reduce job manager 
 */
public class JobManager {
  public static void execute(JobConf jobConf, Matrix result) throws IOException {
    try {
      JobClient.runJob(jobConf);
      // TODO : Thinking about more efficient method.
      int rows = result.getColumn(0).size();
      int columns = result.getRow(0).size();
      result.setDimension(rows, columns);
    } catch (IOException e) {
      result.close();
      throw new IOException(e);
    }
  }
  
  /** 
   * a help method to execute a job
   * 
   * @param jobConf
   * @throws IOException
   */
  public static void execute(JobConf jobConf) throws IOException {
    JobClient.runJob(jobConf);
  }
  
}
