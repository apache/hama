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

import cats.data.Kleisli
import cats.data.NonEmptyList
import cats.implicits._
import cats.data._
import scala.util.Either

// TODO: replace with zio.KleisliIO instead.
object Step {

  def apply[I, O](f: I => Result[O]) = new Step[I, O](Kleisli(f))

}
class Step[I, O](k: Kleisli[Result, I, O]) {

  /**
   * Form a new computation by taking in input I, and produce output O1.
   */
  // TODO: rename for name collision preventation.
  def then[O1](next: Step[O, O1]) = new Step[I, O1](
    k andThen sync() andThen next.kleisli
  )

  protected[bsp] def kleisli = k

  def run(in: I) = k.run(in) 

  protected[bsp] def sync() = Kleisli[Result, O, O]( (o: O) => {
    /* TODO: barrier sync() operation */
    Success(o)
  })

}
