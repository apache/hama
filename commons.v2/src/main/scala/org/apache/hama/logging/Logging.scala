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
package org.apache.hama.logging

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

trait Logging[T] {

  def info(message: String)

  def debug(message: String)

  def warn(message: String)

  def trace(message: String)

  def error(message: String)
}

object Logging {

  def slf4j[T: ClassTag: TypeTag](clazz: Class[T]): Logging[Logger] = 
    new Logging[Logger] {

      lazy val log = LoggerFactory.getLogger(clazz)

      override def info(message: String) = if(log.isInfoEnabled)
         log.info(message)

      override def debug(message: String) = if(log.isDebugEnabled)
        log.debug(message)

      override def warn(message: String) = if(log.isWarnEnabled)
        log.warn(message)

      override def trace(message: String) = if(log.isTraceEnabled)
        log.trace(message)

      override def error(message: String) = if(log.isErrorEnabled)
        log.error(message)
    
    }
}
