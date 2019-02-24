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

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import com.typesafe.config.ConfigValue
import com.typesafe.config.ConfigValueFactory
import scala.util.Try

trait Setting {

  def set[T](key: String, value: T): DefaultSetting 

  def get[T](key: String): Option[T] 

}

object Setting {

  val hama = "hama"

  def create(name: String = hama): Setting = DefaultSetting (
    ConfigFactory.load(name)
  )

  // TODO: hadoop configuration to  
  // def from(configuration: Configuration): Setting = ???

}

protected[conf] case class DefaultSetting(config: Config) extends Setting {

  override def set[T](key: String, value: T): DefaultSetting = DefaultSetting (
    config.withValue(key, ConfigValueFactory.fromAnyRef(value))
  )

  override def get[T](key: String): Option[T] = 
    Try(config.getAnyRef(key).asInstanceOf[T]).toOption

}

