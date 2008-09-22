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
import org.apache.hama.shell.HamaShellEnv;

/**
 * Eval Expression.
 * 
 * Deal with all the evalution expression in hama, like:
 * 
 * A = ( LoadOperation | AlgebraOperation )
 * 
 */
public class EvalExpression extends HamaExpression {

  String aliaseName;
  HamaOperation operation;

  public EvalExpression(HamaConfiguration conf, HamaShellEnv env,
      String aliase, HamaOperation operation) {
    super(conf, env);
    this.aliaseName = aliase;
    this.operation = operation;
  }

  @Override
  public void execute() {
    try {
      Object value = operation.operate();
      if (value == null) {
        System.err.println("'null' can't be assigned to alias '" + aliaseName
            + "'");
      } else {
        this.env.setAliase(aliaseName, value);
        System.out.println(aliaseName + " =");
        System.out.println(value);
      }
    } catch (AlgebraOpException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

}
