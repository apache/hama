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

  def set[T](key: String, value: T): Setting

  def get[T](key: String): Option[T] 

  def get[T](key: String, default: T): T 

}
object Setting {

  val hama = "hama"

  def typesafe = Typesafe(ConfigFactory.load(hama))

  case class Typesafe(config: Config) extends Setting {

    override def set[T](key: String, value: T): Setting =  
      Typesafe(config.withValue(key, ConfigValueFactory.fromAnyRef(value)))

    override def get[T](key: String): Option[T] = 
      Try(config.getAnyRef(key).asInstanceOf[T]).toOption

    override def get[T](key: String, default: T): T = get(key).getOrElse(default)

  }

}
