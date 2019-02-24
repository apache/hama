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

import io.aeron.driver.MediaDriver
import org.agrona.concurrent.ShutdownSignalBarrier  
import org.apache.hama.logging.Logging
import org.scalatest._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.Failure
import scala.util.Success
import scala.util.Either
import scalaz.zio._

class CommunicatorSpec extends FlatSpec with Matchers with Logging {

  import Communicator._

  "Communicator with zio.Fiber" should "send/ receive message." in {

    val barrier = new ShutdownSignalBarrier 
    val driverIO = IO.sync{ 
        val driver = Driver.create().start
        barrier.await
        driver
    }

    lazy val communicator = Communicator.create()

    def subscriptionIO(comm: Communicator) = IO.sync { 
      comm.withSubscription().subscription.map { sub => sub.receive { 
        (buffer, offset, length, header) => 
          val data = new Array[Byte](length)
          buffer.getBytes(offset, data) 
          val hello = new String(data)
          log.info(s"Subscription handler receives message: $hello ")
          assert("hello".equals(hello))
          false // stop receive message while loop
      }}
      comm
    }

    def publicationIO(comm: Communicator) = IO.sync { 
      comm.withPublication().publication.map { pub => pub.send (
        messageBytes = "hello".getBytes
      )}
      comm
    }

    val handlers = for {
      dio <- driverIO.fork
      sio <- subscriptionIO(communicator).fork
      pio <- publicationIO(communicator).fork
      cio <- IO.sync { barrier.signal; IO.unit }.fork
      _ <- cio.join
      p <- pio.join
      s <- sio.join
      d <- dio.join
    } yield (p, s, d)

    new RTS {}.unsafeRun { 
      handlers.run 
    }.map { case (p, s, d) =>
      p.close
      s.close
      d.map(_.close)
    }
    log.info("Done!")
  }

/*
  "Communicator" should "send/ receive message." in {
    val barrier = new ShutdownSignalBarrier
    val f1 = Future {
       val driver = Driver.create().start
       barrier.await
       driver
    }
    val communicator = Communicator.create()
    val f2 = Future { 
      communicator.withSubscription().subscription.map { sub => sub.receive { 
        (buffer, offset, length, header) =>
          val data = new Array[Byte](length)
          buffer.getBytes(offset, data) 
          val hello = new String(data)
          log.info(s"Receive string $hello")
          assert("hello".equals(hello))
          false
      }}
      communicator
    }
    val f3 = Future {
      communicator.withPublication().publication.map { pub => pub.send (
        messageBytes = "hello".getBytes
      )}
      communicator
    }
    Thread.sleep(3 * 1000)  
    barrier.signal  
    f2.onComplete {
      case Success(communicator) => communicator.close
      case Failure(ex) => throw ex
    }
    f3.onComplete {
      case Success(communicator) => communicator.close
      case Failure(ex) => throw ex
    }
    f1.onComplete {
      case Success(driver) => driver match {
        case Left(ex) => throw ex
        case Right(d) => d.close
      }
      case Failure(ex) => throw ex
    }
  }
*/

}

