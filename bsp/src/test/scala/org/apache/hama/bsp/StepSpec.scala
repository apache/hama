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
package org.apache.hama.bsp

import org.scalatest._
import scala.collection.mutable.WrappedArray

class StepSpec extends FlatSpec with Matchers {

  "Step" should "be composable" in {

    val expected = Success(Seq('1', '2', '3', '.', '0', '_', 'd', 'o', 'u', 'b', 'l', 'e'))

    val step1 = Step[Int, Double]( (int: Int) => 
      if(int > 0) Success(int.toDouble) else Failure(new Exception(s"$int smaller than 0!"))
    )

    val step2 = Step[Double, String]( (double: Double) => 
      if(double % 10.0 > 0) Success(double.toString + "_double") 
      else Failure(new Exception(s"$double % 10 is 0!"))
    )

    val step3 = Step[String, Seq[Char]]( (str: String) => Success(str.toCharArray.toSeq))

    val result = (step1 then step2 then step3) run 123 

    result.map { wrappedArray => wrappedArray.map { e => 
      assert(expected.value.contains(e))
    }}

  }

}
