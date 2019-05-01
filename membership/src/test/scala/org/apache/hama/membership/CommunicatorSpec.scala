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
import scala.concurrent.duration.Duration
import scala.concurrent.duration.DurationInt
import scalaz.zio._
import scalaz.zio.duration.{ Duration => ZDuration}

class CommunicatorSpec extends FlatSpec with Matchers {

  import Communicator._

  "Communicator" should "send and receive a message." in {

    var hello = ""
    val barrier = new ShutdownSignalBarrier
    val dio = driver(barrier).delay(ZDuration.fromScala(0 seconds))
    val subio = subscriber().receive {
      (buffer: DirectBuffer, offset: Int, length: Int, header: Header) =>
        val data = new Array[Byte](length)
        buffer.getBytes(offset, data)
        hello = new String(data)
        println("Received? "+hello)
        false
    }
    val pubio = publisher().publish (
      messageBytes = "hello".getBytes,
      countLimit = 1024,
      deadline = (System.nanoTime + 10.seconds.toNanos),
      sleep = { () => Thread.sleep(1 * 1000) }
    )

    val handler = for {
      forked_driver <- dio.fork
      forked_sub <- subio.delay(ZDuration.fromScala(3 seconds)).fork
      forked_pub <- pubio.delay(ZDuration.fromScala(5 seconds)).fork
      forked_barrier <- ZIO.effect { barrier.signal; ZIO.unit }.fork
      _ <- forked_barrier.join
      pub <- forked_pub.join
      sub <- forked_sub.join
      driver <- forked_driver.join
    } yield (pub, sub, driver)

    val runtime = new DefaultRuntime {}
    runtime.unsafeRun { handler.run }
    assert("hello".equals(hello))
  }
}
