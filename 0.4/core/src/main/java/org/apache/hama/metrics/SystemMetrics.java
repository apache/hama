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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import static org.apache.hama.metrics.SystemMetrics.Metrics.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hama.HamaConfiguration;

public final class SystemMetrics implements MetricsSource {

  public static final Log LOG = LogFactory.getLog(SystemMetrics.class);
  private final String name, description;
  private final Map<String, MetricsSource> sources;
  private final Map<String, MetricsSink> sinks;

  public enum Metrics{
    NumActiveSources("Number of active metrics sources"),
    NumActiveSinks("Number of active metrics sinks"),
    Hostname("Metrics system hostname");

    private final String desc;

    Metrics(String desc) { this.desc = desc; }
    public String description() { return desc; }

    @Override public String toString(){
      return "name "+name()+" description "+desc;
    }
  }

  public SystemMetrics(final Map<String, MetricsSource> sources, 
      final Map<String, MetricsSink> sinks, HamaConfiguration conf){
    this.name = "SystemMetrics";
    this.description = "System metrics information.";
    this.sources = sources;
    this.sinks = sinks;
  }

  @Override
  public final String name(){
    return this.name;
  }

  @Override
  public final String description(){
    return this.description;
  }

  /**
   * Record metrics data, and collect those information as records.   
   * @param collector of records to be collected.
   */
  public void snapshot(final List<MetricsRecord> collector){
    final MetricsRecord record = new MetricsRecord(name(), description());
    record.add(new Metric(NumActiveSources, sources.size()));
    record.add(new Metric(NumActiveSinks, sinks.size()));
    record.tag(Hostname, DefaultMetricsSystem.hostname());
    collector.add(record);
  }

}
