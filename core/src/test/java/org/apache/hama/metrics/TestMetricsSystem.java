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

import junit.framework.TestCase;
import org.apache.hama.HamaConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class TestMetricsSystem extends TestCase {

  public static final Log LOG = LogFactory.getLog(TestMetricsSystem.class);

  private MetricsSystem metricsSystem;
  private HamaConfiguration conf;

  public void setUp() throws Exception{
    this.conf = new HamaConfiguration();
    this.conf.set("bsp.metrics.conf_files", "hama-metrics-msys.properties");
    this.metricsSystem = MetricsFactory.createMetricsSystem("test", conf);
    assertNotNull("Ensure metrics system is not null.", this.metricsSystem);
    this.metricsSystem.start();
  }

  public void testDefaultMetricsSystem() throws Exception {
    MetricsSource src = MetricsFactory.createSource(JvmMetrics.class, 
      new Object[]{ "processName", "sessionId", conf });
    this.metricsSystem.register(JvmMetrics.class.getName(), 
                                "Test Jvm metrics source.", src);
    // default value the metrics system will periodically harvet 
    // metrics information from source to sink.
    long period = this.metricsSystem.period();
    LOG.info("period (in seconds) the metrics system is configured: "+period);
    Thread.sleep(period * 2000); 
    MetricsSink sink =  this.metricsSystem.findSink(
      "org.apache.hama.metrics.SystemMonitorSink");
    assertNotNull("Sink should be auto registered.", sink);
    MetricsRecord record = ((SystemMonitorSink)sink).get();
    assertNotNull("Check if MetriscRecord has data.", record);
    for(Metric<? extends Number> metric: record.metrics()){
      Number num = metric.value();
      assertTrue("Check metrics data returned.", 
                 num.longValue() > 0 || num.longValue() <= 0);
    }
  }

  public void tearDown() throws Exception {
    this.metricsSystem.stop();
  }
}
