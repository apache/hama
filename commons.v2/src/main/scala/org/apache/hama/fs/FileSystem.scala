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
package org.apache.hama.fs

import java.io.IOException
import java.nio.file.Path
import java.nio.file.Paths
import scala.util.Either
import scala.util.Try

trait FileSystem {

  def close: Either[IOException, Unit]

  def path(paths: String*): Either[Throwable, Path]

  def create(path: Path): Either[Throwable, Path]

  def remove(path: Path): Either[Throwable, Path]

  def exist(path: Path): Boolean 

  def isLocal: Boolean

}

object FileSystem {

  def apply(implicit fs: FileSystem): FileSystem = fs

  def local() = apply(localFS)

  implicit val localFS = new FileSystem {

    override def close: Either[IOException, Unit] = Right()
  
    override def path(paths: String*): Either[Throwable, Path] = 
      Try(Paths.get(paths(0), paths.tail:_*)).toEither

    override def create(path: Path): Either[Throwable, Path] = 
      Try(path.toFile.createNewFile).map(_ => path).toEither

    override def remove(path: Path): Either[Throwable, Path] = 
      Try(path.toFile.delete).map(_ => path).toEither

    override def exist(path: Path): Boolean = path.toFile.exists

    override def isLocal: Boolean = true

  }

}
