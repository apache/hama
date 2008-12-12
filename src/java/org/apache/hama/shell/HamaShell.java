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
package org.apache.hama.shell;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hama.HamaConfiguration;

/**
 * HamaShell
 * 
 */
// TODO : improve to make it executed in batch mode.
// now it parse the input stream to expression by expression.
// so we only can execute expression by expression.
// it is not efficient.
public class HamaShell {

  private final Log log = LogFactory.getLog(getClass());

  HamaShellEnv env;
  HamaShellParser parser;
  HamaConfiguration conf;

  /**
   * HamaShell Constructor.
   * 
   * @param in input stream to be parsed
   * @param shellEnv shell environment
   * @param conf Hama Configuration
   */
  public HamaShell(BufferedReader in, HamaShellEnv shellEnv,
      HamaConfiguration conf) {
    this.env = shellEnv;
    this.conf = conf;

    if (in != null) {
      parser = new HamaShellParser(in, env, this.conf);
    }
  }

  /**
   * HamaShell Executed in interactive mode.
   */
  public void run() {
    parser.setInteractive(true);
    parser.parseContOnError();
  }

  /**
   * HamaShell Executed in batch mode.
   */
  public void exec() {
    try {
      parser.setInteractive(false);
      parser.parseStopOnError();
    } catch (Exception e) {
      ByteArrayOutputStream bs = new ByteArrayOutputStream();
      e.printStackTrace(new PrintStream(bs));
      log.error(bs.toString());
      log.error(e.getMessage());
    }
  }

  public static void main(String[] args) {
    HamaShellEnv shellEnv = new HamaShellEnv();
    HamaShell shell = new HamaShell(new BufferedReader(new InputStreamReader(
        System.in)), shellEnv, new HamaConfiguration());
    shell.run();
    shellEnv.clearAliases();
  }

}
