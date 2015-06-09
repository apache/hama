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

import static org.junit.Assert.assertEquals;

import java.util.Random;

import org.junit.Test;

/**
 * Test case for {@link FunctionFactory}
 * 
 */
public class TestFunctionFactory {

  @Test
  public void testCreateDoubleFunction() {
    double input = 0.8;
    
    String sigmoidName = "Sigmoid";
    DoubleFunction sigmoidFunction = FunctionFactory
        .createDoubleFunction(sigmoidName);
    assertEquals(sigmoidName, sigmoidFunction.getFunctionName());
    
    double sigmoidExcepted = 0.68997448;
    assertEquals(sigmoidExcepted, sigmoidFunction.apply(input), 0.000001);
    
    
    String tanhName = "Tanh";
    DoubleFunction tanhFunction = FunctionFactory.createDoubleFunction(tanhName);
    assertEquals(tanhName, tanhFunction.getFunctionName());
    
    double tanhExpected = 0.66403677;
    assertEquals(tanhExpected, tanhFunction.apply(input), 0.00001);
    
    
    String identityFunctionName = "IdentityFunction";
    DoubleFunction identityFunction = FunctionFactory.createDoubleFunction(identityFunctionName);
    
    Random rnd = new Random();
    double identityExpected = rnd.nextDouble();
    assertEquals(identityExpected, identityFunction.apply(identityExpected), 0.000001);
  }
  
  @Test
  public void testCreateDoubleDoubleFunction() {
    double target = 0.5;
    double output = 0.8;
    
    String squaredErrorName = "SquaredError";
    DoubleDoubleFunction squaredErrorFunction = FunctionFactory.createDoubleDoubleFunction(squaredErrorName);
    assertEquals(squaredErrorName, squaredErrorFunction.getFunctionName());
    
    double squaredErrorExpected = 0.045;
    
    assertEquals(squaredErrorExpected, squaredErrorFunction.apply(target, output), 0.000001);
    
    String crossEntropyName = "CrossEntropy";
    DoubleDoubleFunction crossEntropyFunction = FunctionFactory.createDoubleDoubleFunction(crossEntropyName);
    assertEquals(crossEntropyName, crossEntropyFunction.getFunctionName());
    
    double crossEntropyExpected = 0.91629;
    assertEquals(crossEntropyExpected, crossEntropyFunction.apply(target, output), 0.000001);
  }

}
