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
    System.out.println("HAMA V1.0 SHELL COMMANDS");
    System.out.println("all the supported commands in hama shell will be:");
    System.out.println("'<hama expression> ;',");
    System.out.println("'quit',");
    System.out.println("'help'.");
    System.out.println();
    System.out.println("Hama Expressions:");
    System.out.println("Load\t\tload a matrix from a specified source. but now is unsupported.");
    System.out.println();
    System.out.println("Save\t\tsave a matrix to a specified target. but now is unsupported.");
    System.out.println();
    System.out.println("Add\t\tMatrix Addition.");
    System.out.println("\t\thama> a = b + c;");
    System.out.println();
    System.out.println("Sub\t\tMatrix Subtraction.");
    System.out.println("\t\thama> a = b - c;");
    System.out.println();
    System.out.println("Multiply\tMatrix Multiplication.");
    System.out.println("\t\thama> a = b * c;");
    System.out.println();
    System.out.println("Random\t\tGenerate a random matrix.");
    System.out.println("\t\thama> a = matrix.random 10 10;");
    System.out.println();
    System.out.println("Examples");
    System.out.println("hama> a = matrix.random 10 10;");
    System.out.println("hama> b = matrix.random 10 10;");
    System.out.println("hama> c = a + b;");
    System.out.println("hama> d = (a + c) * ( b + a * c );");
    System.out.println();
    System.out.println("Commands:");
    System.out.println("quit -- exit the hama shell");
    System.out.println("help -- dispaly the usage informations");
    System.out.println();
    System.out.println("For more on the Hama Shell, see http://wiki.apache.org/hama/Shell");
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
