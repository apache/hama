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

import cats.data.{ State => CatsState, Reader }
import cats.syntax._
import cats.implicits._
import org.apache.hama.conf.Setting
import org.apache.hama.fs.FileSystem

object BSPMaster {

  object Event {

    case object CommunicatorReady extends Event
    case object SchedulerReady extends Event
    case object BarrierSynchronizationReady extends Event
    case object MonitorReady extends Event
    case object StorageReady extends Event

    case object CommunicatorFailure extends Event
    case object SchedulerFailure extends Event
    case object BarrierSynchronizationFailure extends Event
    case object MonitorFailure extends Event
    case object StorageFailure extends Event

    case object CommunicatorShuttingDown extends Event
    case object SchedulerShuttingDown extends Event
    case object BarrierSynchronizationShuttingDown extends Event
    case object MonitorShuttingDown extends Event
    case object StorageShuttingDown extends Event

    case object CommunicatorStopped extends Event
    case object SchedulerStopped extends Event
    case object BarrierSynchronizationStopped extends Event
    case object MonitorStopped extends Event
    case object StorageStopped extends Event

    val ready = Set (
      CommunicatorReady, SchedulerReady, BarrierSynchronizationReady, 
      MonitorReady, StorageReady
    )

    val failures = Set (
      CommunicatorFailure, SchedulerFailure, BarrierSynchronizationFailure, 
      MonitorFailure, StorageFailure
    )

    val shuttingDown = Set (
      CommunicatorShuttingDown, SchedulerShuttingDown,
      BarrierSynchronizationShuttingDown, MonitorShuttingDown, 
      StorageShuttingDown
    )

    val stopped = Set(
      CommunicatorStopped, SchedulerStopped, BarrierSynchronizationStopped, 
      MonitorStopped, StorageStopped
    )
  }
  trait Event extends Product with Serializable

  object State {

    type NextState[N] = CatsState[Either[Failure, N], Unit]

    import Event._

    case object Stopped extends State
    case object Starting extends State
    case object Running extends State 
    case object ShuttingDown extends State
    trait Failure extends State 
    case class IllegalState(expected: State, actual: State) extends Failure
    case object ServiceFailure extends Failure // TODO: change to case class with events
    case class Faulty(prev: Failure) extends Failure 
    case object Recovering extends State

    /**
     * Entry state.
     */
    def stopped2Starting: NextState[State] = CatsState { s => s match {
      case Right(currentState) => currentState match {
        case Stopped => (Right(Starting), Unit) 
        case actual@_ => (Left(IllegalState(Stopped, actual)), Unit)
      }
      case Left(prev) => (Left(Faulty(prev)), Unit)
    }}

    /**
     * Transit from Starting to either Starting, Running, or Failure.
     */
    def starting2Running(events: Set[Event]): NextState[State] = 
      CatsState { s => s match {
        case Right(currentState) => currentState match {
          case Starting => 
            if(ready.equals(events)) (Right(Running), Unit) 
            else if(!failures.intersect(events).isEmpty) (Left(ServiceFailure), Unit)
            else (Right(Starting), Unit) 
          case actual@_ => (Left(IllegalState(Starting, actual)), Unit)
        }
        case Left(prev) => (Left(Faulty(prev)), Unit)
      }}

    /**
     * Transit from Running to either Running, ShuttingDown, or Failure.
     */
    def running2ShuttingDown(events: Set[Event]): NextState[State] = 
      CatsState { s => s match {
        case Right(currentState) => currentState match {
          case Running => 
            if(!shuttingDown.intersect(events).isEmpty) (Right(ShuttingDown), Unit)
            else if(!failures.intersect(events).isEmpty) (Left(ServiceFailure), Unit)
            else (Right(Running), Unit) 
          case actual@_ => (Left(IllegalState(Running, actual)), Unit)
        }
        case Left(prev) => (Left(Faulty(prev)), Unit)
      }}

    def shuttingDown2Stopped(events: Set[Event]): NextState[State] =
      CatsState { s => s match {
        case Right(currentState) => currentState match {
          case ShuttingDown =>
            if(stopped.equals(events)) (Right(Stopped), Unit)
            else if(!failures.intersect(events).isEmpty) (Left(ServiceFailure), Unit)
            else (Right(ShuttingDown), Unit)
          case actual@_ => (Left(IllegalState(ShuttingDown, actual)), Unit)
        }
        case Left(prev) => (Left(Faulty(prev)), Unit)
      }}
    
  }
  sealed trait State extends Product with Serializable

  def build = Reader{ (setting: Setting) => 
    val host = setting.get("org.apache.hama.net.host", "localhost")
    val port = setting.get("org.apache.hama.net.port", -1)
    (Address.create(host, port),
    Identifier.create(host, port),
    Master.validNel,
    FileSystem.local.validNel).mapN(BSPMaster.apply)
  }

}
final case class BSPMaster (
  address: Address, id: Identifier, role: Role, fs: FileSystem
)
