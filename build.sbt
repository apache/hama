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
lazy val common = Seq (
  version := "0.0.1",
  organization := "org.apache.hama",
  scalaVersion := "2.12.6",
  scalacOptions := Seq ("-deprecation", "-Ypartial-unification"), 
  libraryDependencies ++= Seq (
    "org.typelevel" %% "cats-core" % "1.6.0",
    "org.scalaz" %% "scalaz-zio" % "1.0-RC4",
    "org.scalatest" %% "scalatest" % "3.0.5" % "test"
  )
)

lazy val commons = (project in file("commons.v2")).settings (
  common
)

lazy val bsp = (project in file("bsp")).settings (
  common
)

lazy val membership = (project in file("membership")).settings (
  common
) .dependsOn(commons)

lazy val core = (project in file(".")).aggregate(commons, bsp, membership)
