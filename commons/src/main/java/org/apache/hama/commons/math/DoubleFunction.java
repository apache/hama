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
package org.apache.hama.commons.math;

/**
 * A double double function takes two arguments. A vector or matrix can apply
 * the double function to each element.
 * 
 */
public abstract class DoubleFunction extends Function {

  /**
   * Apply the function to element.
   * 
   * @param elem The element that the function apply to.
   * @return The result after applying the function.
   */
  public abstract double apply(double value);

  /**
   * Apply the gradient of the function.
   * 
   * @param elem
   * @return
   */
  public abstract double applyDerivative(double value);

}
