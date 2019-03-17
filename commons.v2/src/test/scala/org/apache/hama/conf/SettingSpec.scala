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
package org.apache.hama.conf

import org.scalatest._

class SettingSpec extends FlatSpec with Matchers {

  "Setting" should "get/ set value" in {
    val typesafe = Setting.typesafe
    val newSetting = typesafe.set[String]("db.host", "host1").
                              set[Int]("db.port", 1234).
                              set[Double]("cpu.load", 1.2d)
    assert(Some("host1") == newSetting.get("db.host"))
    assert(Some(1234) == newSetting.get("db.port"))
    assert(Some(1.2d) == newSetting.get("cpu.load"))
  }

}
