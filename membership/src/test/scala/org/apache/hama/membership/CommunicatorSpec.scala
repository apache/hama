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
package org.apache.hama.membership

import io.aeron.logbuffer.Header
import org.scalatest._
import org.agrona.DirectBuffer
import org.agrona.concurrent.ShutdownSignalBarrier
import scalaz.zio._

class CommunicatorSpec extends FlatSpec with Matchers {

  import Communicator._

  "Communicator" should "" in {


    val barrier = new ShutdownSignalBarrier
    val dio = driver(barrier)
    val a = aeron
    val subio = a.subscriber().receive { 
      (buffer: DirectBuffer, offset: Int, length: Int, header: Header) =>
        val data = new Array[Byte](length)
        buffer.getBytes(offset, data)
        val hello = new String(data)
        false
    }.flatMap(_.close)
    val pubio = a.publisher().publish(messageBytes = "hello".getBytes).flatMap(_.close)

    val handler = for {
      forked_d <- dio.fork
      forked_sub <- subio.fork
      forked_pub <- pubio.fork
      forked_b <- ZIO.effect { barrier.signal; ZIO.unit }.fork
      _ <- forked_b.join
      p <- forked_pub.join
      s <- forked_sub.join
      d <- forked_d.join
    } yield (p, s, d)

    val runtime = new DefaultRuntime {}
    runtime.unsafeRun { handler.run }
  }
}
