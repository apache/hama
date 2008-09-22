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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.shell.execution.HamaExpression;
import org.apache.hama.shell.parser.expression.HamaExpressionParser;
import org.apache.hama.shell.parser.script.HamaScriptParser;
import org.apache.hama.shell.parser.script.HamaScriptParserTokenManager;

public class HamaShellParser extends HamaScriptParser {

  private final Log log = LogFactory.getLog(getClass());

  // flag to identify that the shell script has been finished.
  boolean mFinished = false;

  HamaShellEnv shellEnv;
  HamaConfiguration conf;

  public HamaShellParser(HamaScriptParserTokenManager tm, HamaShellEnv env,
      HamaConfiguration conf) {
    super(tm);
    init(env, conf);
  }

  public HamaShellParser(InputStream stream, String encoding, HamaShellEnv env,
      HamaConfiguration conf) {
    super(stream, encoding);
    init(env, conf);
  }

  public HamaShellParser(InputStream stream, HamaShellEnv env,
      HamaConfiguration conf) {
    super(stream);
    init(env, conf);
  }

  public HamaShellParser(Reader stream, HamaShellEnv env, HamaConfiguration conf) {
    super(stream);
    init(env, conf);
  }

  private void init(HamaShellEnv env, HamaConfiguration conf) {
    this.shellEnv = env;
    this.conf = conf;
  }

  /**
   * use HamaExpressionParser to parser a hama script expression. such as "a = b +
   * c;", "a = load 'file' as matrix using 'class' map 4;" ...
   */
  @Override
  protected void processScript(String cmd) throws IOException {
    if (cmd.charAt(cmd.length() - 1) != ';')
      cmd = cmd + ";";
    HamaExpressionParser t = new HamaExpressionParser(new ByteArrayInputStream(
        cmd.getBytes()), this.shellEnv, this.conf);
    HamaExpression n;
    try {
      n = t.Start();
      if (n != null)
        n.execute();
    } catch (org.apache.hama.shell.parser.expression.ParseException e) {
      e.printStackTrace();
    }
  }

  @Override
  protected void printHelp() {
    System.out.println("Operations:");
    System.out.println("<hama operation>;");
    System.out.println("such as : \"A = B + C\" or \"save A as file\"");
    System.out.println();
    System.out.println("Commands:");
    System.out.println("quit -- exit the hama shell");
    System.out.println("help -- dispaly the usage informations");
  }

  /**
   * Display the prompt of hama shell.
   */
  @Override
  public void prompt() {
    if (mInteractive) {
      System.out.print("Hama > ");
      System.out.flush();
    }
  }

  @Override
  protected void quit() {
    mFinished = true;
  }

  /**
   * Used in HamaShell Batch mode.
   * 
   * @throws IOException
   * @throws org.apache.hama.shell.parser.expression.ParseException
   * @throws org.apache.hama.shell.parser.script.ParseException
   */
  public void parseStopOnError() throws IOException,
      org.apache.hama.shell.parser.expression.ParseException,
      org.apache.hama.shell.parser.script.ParseException {
    prompt();
    mFinished = false;
    while (!mFinished)
      parse();
  }

  /**
   * Used in HamaShell Interactive mode.
   */
  public void parseContOnError() {
    prompt();
    mFinished = false;
    while (!mFinished)
      try {
        parse();
      } catch (Exception e) {
        log.error(e.toString());
      }
  }

}
