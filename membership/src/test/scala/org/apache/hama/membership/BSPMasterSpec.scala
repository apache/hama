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

import org.scalatest._
import org.apache.hama.logging.Logging

class BSPMasterSpec extends FlatSpec with Matchers {

  val log = Logging.slf4j(getClass)

  import BSPMaster._
  import BSPMaster.State._
  import BSPMaster.Event._

  "BSPMaster State" should "transit sequentially." in {
    val starting = for {
      s <- stopped2Starting
    } yield s
    val startingState = starting.run(Right(Stopped)).value._1
    log.info(s"Actual BSPMaster state: $startingState")
    assert(Right(Starting).equals(startingState))

    val running = for {
      r <- starting2Running(ready)
    } yield r
    val runningState = running.run(startingState).value._1
    log.info(s"Actual BSPMaster state: $runningState")
    assert(Right(Running).equals(runningState))

    val shuttingdown = for {
      sd <- running2ShuttingDown(Set(CommunicatorShuttingDown))
    } yield sd
    val shuttingDownState = shuttingdown.run(runningState).value._1
    log.info(s"Actual BSPMaster state: $shuttingDownState")
    assert(Right(ShuttingDown).equals(shuttingDownState))

    val _stopped_ = for {
      s <- shuttingDown2Stopped(stopped)
    } yield s
    val stoppedState = _stopped_.run(shuttingDownState).value._1
    log.info(s"Actual BSPMaster state: $stoppedState")
    assert(Right(Stopped).equals(stoppedState))
      
  }


}

