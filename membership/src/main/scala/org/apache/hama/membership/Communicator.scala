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

  def driver: Task[MediaDriver] = ZIO.effect {
    val context = new MediaDriver.Context().
      threadingMode(DEDICATED).
      conductorIdleStrategy(new BusySpinIdleStrategy).
      receiverIdleStrategy(new BusySpinIdleStrategy).
      senderIdleStrategy(new BusySpinIdleStrategy)
    MediaDriver.launch(context)
  }

  def aeron: Task[Aeron] = ZIO.effect {
    val context = new Aeron.Context()    
    Aeron.connect(context) 
  }

  def subscriber(aeron: Task[Aeron]): Task[Subscription] = aeron.flatMap { a =>
    Task.effect(a.addSubscription("aeron:udp?endpoint=localhost:12345", 10))
  }

  def publisher(aeron: Task[Aeron]): Task[Publication] = aeron.flatMap { a => 
    Task.effect(a.addPublication("aeron:udp?endpoint=localhost:12344", 10))
  }

}
