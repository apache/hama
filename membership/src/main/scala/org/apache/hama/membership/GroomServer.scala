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

import org.apache.hama.conf.Setting
import org.apache.hama.fs.FileSystem

object GroomServer {

  object Event {
    
  }
  trait Event extends Product with Serializable

  object State {

    /* Groom is stopped. */
    case object Stopped extends State

    /* Groom is running. */
    case object Running extends State

    /* Disk stale. */
    case object Stale extends State

    /* Can't connect to Master */
    case object Denied extends State

    case object ShuttingDown extends State

  }
  sealed trait State extends Product with Serializable

}
case class GroomServer (
  address: Address, id: Identifier, role: Role, fs: FileSystem
)
