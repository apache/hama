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
 * Get the squashing function according to the name.
 */
public class SquashingFunctionFactory {

  /**
   * Get the squashing function instance according to the name. If no matched
   * squahsing function is found, return the sigmoid squashing function by
   * default.
   * 
   * @param name The name of the squashing function.
   * @return The instance of the squashing function.
   */
  public static SquashingFunction getSquashingFunction(String name) {
    if (name.equalsIgnoreCase("Sigmoid")) {
      return new Sigmoid();
    } else if (name.equalsIgnoreCase("Tanh")) {
      return new Tanh();
    }
    throw new IllegalStateException(String.format(
        "No squashing function with name '%s' found.", name));
  }

}
