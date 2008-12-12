/*
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
package org.apache.hama.shell.execution;

import java.io.IOException;

import org.apache.hama.HamaConfiguration;

/**
 * Load Matrix from a file in HDFS.
 * 
 * A = load 'file' as matrix using 'class' ;
 *
 */
public class LoadOperation extends HamaOperation {

  // unsupported now.
//  String filename;
//  String classname;
  
  public LoadOperation(HamaConfiguration conf, String filename, String typename, 
      String classname) {
    super(conf);
//    this.filename = filename;
//    this.classname = classname;
  }
  
  public LoadOperation(HamaConfiguration conf, String filename, String typename, 
      String classname, int map, int reduce) {
    super(conf, map, reduce);
//    this.filename = filename;
//    this.classname = classname;
  }

  public Object operate() throws IOException {
    throw new UnsupportedOperationException("*load* is not supported now.");
  }

}
