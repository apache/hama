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
import io.aeron.logbuffer.Header
import java.util.concurrent.atomic.AtomicBoolean
import org.agrona.BitUtil._
import org.agrona.BufferUtil
import org.agrona.DirectBuffer
import org.agrona.concurrent.BusySpinIdleStrategy
import org.agrona.concurrent.SigInt
import org.agrona.concurrent.UnsafeBuffer
import org.apache.hama.logging.Logging
import org.apache.hama.util.Utils._
import scala.annotation.tailrec
import scala.concurrent.duration.Duration
import scala.concurrent.duration.DurationInt
import scala.util.Either

object Communicator {

  lazy val defaultAeronContext = new Aeron.Context()

  lazy val defaultChannel = Channel()

  final case object BackPressured extends RuntimeException (
    "Failure due to back pressure!"
  )

  final case object NotConnected extends RuntimeException (
    s"Failure because publisher is not connected to subscriber!"
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

  final case class Channel(host: String = localhost, port: Int = 12345) {

    require(!host.isBlank, "Channel's host value is not presented!")

    require(port.isValid, s"Invalid Channel port value: $port!")

    protected[membership] val channel = 
      (host: String, port: Int) => s"aeron:udp?endpoint=$host:$port"

    override def toString(): String = channel(host, port)
  }

  final case class Publisher(publication: Publication) extends Logging {

    require(null != publication, "Aeron Publication is not presented!")

    def isConnected (
      implicit _deadline: Long = (System.nanoTime + 3.seconds.toNanos),
      countLimit: Int = 3, sleep: () => Unit = { () => Thread.sleep(1) }
    ): Boolean = {
      @tailrec
      def _isConnected(pub: Publication, times: Int = 0): Boolean = {
        val connected = pub.isConnected
        if(countLimit > times && (false == connected)) {
          if(System.nanoTime >= _deadline) false else {
            sleep() 
            _isConnected(pub, times + 1)
          }
        } else connected
      }
      _isConnected(publication)
    }

    def send (
      messageBytes: Array[Byte],
      bufferCapacity: Int = 512, 
      boundaryAlighment: Int = CACHE_LINE_LENGTH,
      deadline: Long = (System.nanoTime + 3.seconds.toNanos),
      sleep: () => Unit = { () => Thread.sleep(1) }
    ): Either[Throwable, Publisher] = {
      // TODO: replace with isConnected
      while(!publication.isConnected()) { 
        if(System.nanoTime >= deadline) { 
          return Left(new RuntimeException (
            s"Can't connect to Publication(${publication.channel}, ${publication.streamId})" 
          )) 
        }
        sleep()
      }
      val buffer = new UnsafeBuffer(BufferUtil.allocateDirectAligned (
        bufferCapacity, boundaryAlighment
      ))
      buffer.putBytes(0, messageBytes)
      val result = publication.offer(buffer, 0, messageBytes.length)
      log.debug(s"Publication.offer() returns result $result") 
      result match {
        case res if res < 0L && BACK_PRESSURED == res => Left(BackPressured)
        case res if res < 0L && NOT_CONNECTED == res => Left(NotConnected)
        case res if res < 0L && ADMIN_ACTION == res => Left(AdminAction)
        case res if res < 0L && CLOSED == res => Left(Closed)
        case res if res < 0L && MAX_POSITION_EXCEEDED == res => Left(MaxPositionExceeded)
        case res if res < 0L => Left(UnknownReason)
        case _ => Right(this)
      }
    } 

    def run[O](f: Publication => O): O = f(publication)

    def close() = try {} finally { publication.close }

  }

  final case class Subscriber(subscription: Subscription) extends Logging {

    require(null != subscription, "Aeron Subscription is not presented!")

    def receive(f: (DirectBuffer, Int, Int, Header) => Boolean) {
      var continuous = true 
      val handler = new FragmentAssembler({ 
        (buffer: DirectBuffer, offset: Int, length: Int, header: Header) => {
          val shouldContinuous = f(buffer, offset, length, header)
          continuous = shouldContinuous
        }
      })
      val idleStrategy = new BusySpinIdleStrategy()
      while(continuous) {
        val read = subscription.poll(handler, 10)
        idleStrategy.idle(read)
      }
    }

    def run[O](f: Subscription => O): O = f(subscription)

    def close = try {} finally { subscription.close }

  }

  def create(ctx: Aeron.Context = defaultAeronContext) = 
    Communicator(aeron = Aeron.connect(ctx))

}

/**
 * Aeron client should only be created one per vm for different channels.
 * Otherwise vm may crash with error
 * {{{
 *   org.agrona.concurrent.status.UnsafeBufferPosition.getVolatile() SIGSEGV 
 * }}}
 */
final case class Communicator (
  aeron: Aeron, 
  publication: Option[Communicator.Publisher] = None, 
  subscription: Option[Communicator.Subscriber] = None
) extends Logging {

  import Communicator._

  require(null != aeron, "Aeron instance is not presented!")

  def withPublication (
    channel: Channel = defaultChannel, streamId: Int = 10
  ) = this.copy(publication = Option (
    Publisher(aeron.addPublication(channel.toString, streamId))
  ))

  def withSubscription (
    channel: Channel = defaultChannel, streamId: Int = 10
  ) = this.copy(subscription = Option (
    Subscriber(aeron.addSubscription(channel.toString, streamId))
  ))

  def close() = try {} finally {
    subscription.map(_.close)
    publication.map(_.close)
    if(!aeron.isClosed) aeron.close
  }
}
