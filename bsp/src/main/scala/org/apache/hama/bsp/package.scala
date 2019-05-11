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
package org.apache.hama

import cats.FlatMap
import cats.data.NonEmptyList
import scala.util.Either
import scala.annotation.tailrec

package object bsp {

  type WithSideEffect[A] = Either[NonEmptyList[Seq[Throwable]], A]

  sealed class Result[A](withSideEffect: WithSideEffect[A]) {

    def map[B](f: A => B): Result[B] = this match {
      case Success(s) => Success(f(s))
      case _ => this.asInstanceOf[Result[B]]
    }

    def flatMap[B](f: A => Result[B]): Result[B] = this match {
      case Success(s) => f(s)
      case _ => this.asInstanceOf[Result[B]]
    } 
  }

  final case class Failure[A](exs: Throwable*) extends Result[A](Left(NonEmptyList.of(exs)))

  final case class Success[A](value: A) extends Result(Right(value))

  implicit val resultFlatMap: FlatMap[Result] = new FlatMap[Result] {

    def map[A, B](fa: Result[A])(f: A => B): Result[B] = fa.map(f)

    def flatMap[A, B](fa: Result[A])(f: A => Result[B]): Result[B] = fa.flatMap(f)

    @tailrec
    def tailRecM[A, B](a: A)(f: A => Result[Either[A, B]]): Result[B] = f(a) match {
      case Failure(exs) => Failure(exs)
      case Success(Left(a1)) => tailRecM(a1)(f)
      case Success(Right(b)) => Success(b)
    }

  }

  object State {

    case object LocalComputation extends State

    trait BarrierSynchronization extends State

    case object Begin extends BarrierSynchronization 

    case object End extends BarrierSynchronization 

    case object Communication extends State

  }
  sealed trait State extends Product with Serializable

}

