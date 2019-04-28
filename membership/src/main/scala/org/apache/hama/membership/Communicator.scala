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

import io.aeron.Aeron
import io.aeron.FragmentAssembler
import io.aeron.Publication
import io.aeron.Publication._
import io.aeron.Subscription
import io.aeron.driver.MediaDriver
import io.aeron.driver.ThreadingMode._
import io.aeron.logbuffer.Header
import java.util.concurrent.atomic.AtomicBoolean
import org.agrona.BitUtil._
import org.agrona.BufferUtil
import org.agrona.DirectBuffer
import org.agrona.concurrent.BusySpinIdleStrategy
import org.agrona.concurrent.SigInt
import org.agrona.concurrent.ShutdownSignalBarrier
import org.agrona.concurrent.UnsafeBuffer
import org.apache.hama.logging.Logging
import org.apache.hama.util.Utils._
import scala.annotation.tailrec
import scala.concurrent.duration.Duration
import scala.concurrent.duration.DurationInt
import scala.util.Either

import scalaz.zio._

trait Communicator
object Communicator {

  val localhost = "localhost"

  final case object BackPressured extends RuntimeException (
    "Failure due to back pressure!"
  )

  final case object NotConnected extends RuntimeException (
    "Failure because publisher is not connected to subscriber!"
  )

  final case object AdminAction extends RuntimeException (
    "Failure because of administration action!"
  )
   
  final case object Closed extends RuntimeException (
    "Failure because publication is closed!"
  )

  final case object MaxPositionExceeded extends RuntimeException (
    "Failure due to publication reaching max position!"
  )

  final case object UnknownReason extends RuntimeException (
    "Failure due to unknown reason!"
  )

  case class Channel(host: String = localhost, port: Int = 12345) {
    override def toString(): String = s"aeron:udp?endpoint=$host:$port"
  }

  implicit class SubscriberOps(aeron: Task[Aeron]) {
    def subscriber(channel: Channel = Channel(), streamId: Int = 10): 
      Task[Subscription] = aeron.flatMap { a => 
        Task.effect(a.addSubscription(channel.toString, streamId))
      }
  }

  implicit class ReceiveOps(subscriber: Task[Subscription]) {
    def receive(f: (DirectBuffer, Int, Int, Header) => Boolean) = ZIO.effect {
      var continuous = true 
      val handler = new FragmentAssembler({ 
        (buffer: DirectBuffer, offset: Int, length: Int, header: Header) => 
          val shouldContinuous = f(buffer, offset, length, header)
          continuous = shouldContinuous
      })
      val idleStrategy = new BusySpinIdleStrategy()
      while(continuous) subscriber.flatMap { s =>
        val read = s.poll(handler, 10)
        idleStrategy.idle(read)
        ZIO.unit
      }
      this
    }

    def close() = subscriber.flatMap { sub => sub.close; ZIO.unit }
    
  }

  implicit class PublisherOps(aeron: Task[Aeron]) {
    def publisher(channel: Channel = Channel(), streamId: Int = 10): 
      Task[Publication] = aeron.flatMap { a => 
        Task.effect(a.addPublication(channel.toString, streamId)) 
      }
  }

  implicit class PublishOps(publisher: Task[Publication]) {
    def publish (
      messageBytes: Array[Byte],
      bufferCapacity: Int = 512,
      boundaryAlighment: Int = CACHE_LINE_LENGTH,
      deadline: Long = (System.nanoTime + 3.seconds.toNanos),
      sleep: () => Unit = { () => Thread.sleep(1) }
    ): ZIO[Any, Throwable, PublishOps] = publisher.flatMap { pub => 
      while(!pub.isConnected) {
        if(System.nanoTime >= deadline) return ZIO.fail(new RuntimeException (
          s"Publication can't connect: ${pub.toString}"
        ))
        sleep()
      }
      val buffer = new UnsafeBuffer(BufferUtil.allocateDirectAligned (
        bufferCapacity, boundaryAlighment
      ))
      buffer.putBytes(0, messageBytes)
      val result = pub.offer(buffer, 0, messageBytes.length)
      result match {
        case res if res < 0L && BACK_PRESSURED == res => ZIO.fail(BackPressured)
        case res if res < 0L && NOT_CONNECTED == res => ZIO.fail(NotConnected)
        case res if res < 0L && ADMIN_ACTION == res => ZIO.fail(AdminAction)
        case res if res < 0L && CLOSED == res => ZIO.fail(Closed)
        case res if res < 0L && MAX_POSITION_EXCEEDED == res => ZIO.fail(
          MaxPositionExceeded
        )
        case res if res < 0L => ZIO.fail(UnknownReason)
        case _ => ZIO.succeed(this)
      }
    }

    def close() = publisher.flatMap { pub => pub.close; ZIO.unit }
  }

  def driver(barrier: ShutdownSignalBarrier): Task[MediaDriver] = ZIO.effect {
    val context = new MediaDriver.Context().
      threadingMode(DEDICATED).
      conductorIdleStrategy(new BusySpinIdleStrategy).
      receiverIdleStrategy(new BusySpinIdleStrategy).
      senderIdleStrategy(new BusySpinIdleStrategy)
    val driver = MediaDriver.launch(context)
    barrier.await
    driver
  }

  def aeron: Task[Aeron] = ZIO.effect {
    val context = new Aeron.Context()    
    Aeron.connect(context) 
  }

}
