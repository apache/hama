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
package org.apache.hama.ml.perception;

/**
 * The cost function factory that generates the cost function by name.
 */
public class CostFunctionFactory {

  /**
   * Get the cost function according to the name. If no matched cost function is
   * found, return the SquaredError by default.
   * 
   * @param name The name of the cost function.
   * @return The cost function instance.
   */
  public static CostFunction getCostFunction(String name) {
    if (name.equalsIgnoreCase("SquaredError")) {
      return new SquaredError();
    } else if (name.equalsIgnoreCase("LogisticError")) {
      return new LogisticCostFunction();
    }
    return new SquaredError();
  }
}
