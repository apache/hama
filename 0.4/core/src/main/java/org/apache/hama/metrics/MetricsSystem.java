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
package org.apache.hama.metrics;

/** 
 * A timer thread periodically collects metric information from MetricsSource,
 * and then put data to MetricsSink.
 */
public interface MetricsSystem { 

  /**
   * Prefix value the current metrics system holds.
   * @return String of prefix value.
   */
  String prefix();

  /**
   * A value the metrics system periodically harvests metrics information.
   * @return long value the metrics system is configured.
   */
  long period();

  /**
   * Start the metric system.
   */
  void start();

  /**
   * Stop the metric system.
   */
  void stop();

  /**
   * Register the metric source with a name.
   * @param name as identification for the source. 
   * @param description describes the purpose. 
   * @param source to be used for collecting metrics.
   */
  void register(String name, String description, MetricsSource source);

  /**
   * Register the metric sink with a name.
   * @param name as identification for the sink. 
   * @param description describes the purpose. 
   * @param sink to be used for consuming metrics.
   */
  void register(String name, String description, MetricsSink sink);

  /**
   * Find MetricsSink with corresponded name.
   * @param name of sink to be searched.
   * @return MetricsSink in cache.
   */
  MetricsSink findSink(String name);

  /**
   * Find MetricsSource with corresponded name.
   * @param name of source to be searched.
   * @return MetricsSource in cache.
   */
  MetricsSource findSource(String name);

}
