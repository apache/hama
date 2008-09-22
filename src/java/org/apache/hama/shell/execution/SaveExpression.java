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

import org.apache.hama.HamaConfiguration;
import org.apache.hama.Matrix;
import org.apache.hama.shell.HamaShellEnv;

public class SaveExpression extends HamaExpression {

  Object value;
  String filename;

  public SaveExpression(HamaConfiguration conf, HamaShellEnv env, Object value,
      String fileName) {
    super(conf, env);
    this.value = value;
    this.filename = fileName;
  }

  @Override
  public void execute() {
    System.out.println("it is a save expression.");
    System.out.println("filename=" + filename);
    if (value instanceof Matrix) {
      System.out.println((Matrix) value);
    } else if (value instanceof Double) {
      System.out.println((Double) value);
    } else {
      System.err.println("Unrecognized Type in Save Expression!");
    }
    System.err.println("now *save* operation just print the value.");
  }

}
