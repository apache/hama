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
package org.apache.hama.io

import java.io.DataInput
import java.io.DataOutput
import scala.util.Failure
import scala.util.Success
import scala.util.Try

trait serialization[T] {

  def read(input: DataInput, clazz: Class[T]): T

  def write(value: T): Either[Throwable, Array[Byte]]

}
object serialization {

  def apply[T](implicit instance: T) = instance

  implicit val int = new serialization[Int] {

    override def read(input: DataInput, clazz: Class[Int]): Int = 
      input.readInt
    
    override def write(value: Int): Either[Throwable, Array[Byte]] = Try {
      val byteStream = new ByteArrayOutputStream
      val out = new DataOutputStream(byteSteram)
      out.writeInt(value)
      (out, byteStream)
    } match {
      case Failure(ex) => Left(ex)
      case Success((out, stream)) => 
        try {} finally { out.close }
        Right(stream.toByteArray) 
    }

  }

}
