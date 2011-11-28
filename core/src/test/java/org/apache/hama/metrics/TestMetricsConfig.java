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

import java.util.Properties;
import java.util.Set;

import junit.framework.TestCase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hama.HamaConfiguration;
import static junit.framework.Assert.*;

public class TestMetricsConfig extends TestCase {
  
  public static final Log LOG = LogFactory.getLog(TestMetricsConfig.class); 

  private HamaConfiguration conf;

  public void setUp() throws Exception{
    this.conf = new HamaConfiguration();
  }

  public void testCreate() throws Exception {
    this.conf.set("bsp.metrics.conf_files", 
                  "hama-metrics-notexist.properties, hama-metrics-test-config.properties");
    MetricsConfig config = MetricsConfig.create(this.conf);
    assertNotNull("Test if creating with hama-metrics-test.properties " + 
                  "successfully.", config);
    Set<MetricsConfig.Entry<String, String>> sub = config.subset("bspmaster"); 
    for(MetricsConfig.Entry<String, String> entry: sub){
      String key = entry.key();
      String value = entry.value();
      LOG.info("key "+key+" value "+value);
      if(-1 != key.indexOf("file_jvm")){
        if(-1 != key.indexOf("options")){
          assertEquals("Test if key is bspmaster.sink.file_jvm.options.", 
                       key, "bspmaster.sink.file_jvm.options");
          assertEquals("Test if value is jvm.", value, "jvm");
        }
      }else{
        fail("Key in subset should contain options.");
      }
    }
  }
 
  public void testSubset() throws Exception {
    final Properties prop = new Properties();
    prop.setProperty("test.source.instance.options", "jvm");
    prop.setProperty("test.sink.instance.options", "systemmonitorsink");
    prop.setProperty("bspmaster.source.instance.options", "testbspmastersource");
    prop.setProperty("bspmaster.sink.instance.options", "testbspmastersink");
    prop.setProperty("groom.source.instance.options", "testgroomsource");
    prop.setProperty("groom.sink.instance1.options", "testgroomsink");
    MetricsConfig mc = new MetricsConfig(prop); // only for test
    assertNotNull("Test initialize MetricsConfig instance.", mc);
    LOG.info("Test subset(prefix)");
    Set<MetricsConfig.Entry<String, String>> prefix = mc.subset("test");
    for(MetricsConfig.Entry<String, String> entry: prefix){
      String key = entry.key();
      LOG.info("key:"+key);
      if(-1 != key.indexOf("source")){
        assertEquals("Test key with prefix.", key, 
                   "test.source.instance.options");
      }
      if(-1 != key.indexOf("sink")){
        assertEquals("Test key with prefix.", key, 
                     "test.sink.instance.options");
      }
      String value = entry.value();
      LOG.info("value:"+value);
      if(-1 != key.indexOf("source")){
        assertEquals("Test value with test prefix.", value, "jvm");
      }
      if(-1 != key.indexOf("sink")){
        assertEquals("Test value with test prefix.", value, 
                     "systemmonitorsink");
      }
    }

    LOG.info("Test subset(prefix, type)");
    Set<MetricsConfig.Entry<String, String>> mtype = 
      mc.subset("bspmaster", "sink");
    int count = 0;
    for(MetricsConfig.Entry<String, String> entry: mtype){
      String key = entry.key();
      String value = entry.value();
      LOG.info("key: "+key+" value: "+value);
      if(key.startsWith("bspmaster")) {
        if(-1 != key.indexOf("sink")){
          assertEquals("Test key.", key, "bspmaster.sink.instance.options");
          assertEquals("Test value.", value, "testbspmastersink");
          count++;
        }
      }else {
        fail("Key should start with bspmaster!");
      }
    }
    assertEquals("Test result count for subset(prefix, type).", 
                 count, 1);

    LOG.info("Test subset(prefix, type, instance)");
    Set<MetricsConfig.Entry<String, String>> instances = 
      mc.subset("groom", "source", "instance");
    count = 0;
    for(MetricsConfig.Entry<String, String> entry: instances){
      String key = entry.key();
      String value = entry.value();
      if(key.startsWith("groom")){ 
        if(-1 != key.indexOf("source")) {
          assertEquals("Test key.", key, "groom.source.instance.options");
          assertEquals("Test value.", value, "testgroomsource");
          count++;
        }
      }else{
        fail("Key should start with groom!");
      }
    } 
    assertEquals("Test result count for subset(prefix, type, instance).", 
                 count, 1);
  }

  public void tearDown() throws Exception { }
}
