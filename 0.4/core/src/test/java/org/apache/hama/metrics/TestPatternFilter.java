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

import java.util.Arrays;
import java.util.Properties;
import java.util.List;
import java.util.Set;

import junit.framework.TestCase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.metrics.MetricsConfig.Entry;
import static junit.framework.Assert.*;

public class TestPatternFilter extends TestCase {
  
  public static final Log LOG = LogFactory.getLog(TestPatternFilter.class); 

  private HamaConfiguration conf;

  public void setUp() throws Exception{
    this.conf = new HamaConfiguration();
  }

  public void testEmptyConfig() throws Exception {
    Set<Entry<String, String>> sub = 
      new MetricsConfig(new Properties()).subset("");
    shouldAccept(sub, "anything");
    shouldAccept(sub, tags(newTag("key", "value")));
  }

  public void testIncludeAndIncludeTag() throws Exception {
    final Properties prop = new Properties();
    prop.setProperty("test.sink.include", "garbage");
    prop.setProperty("test.source.include", "foo");
    prop.setProperty("test.source.include.tags", "foo:f");
    final MetricsConfig mc = new MetricsConfig(prop); 
    assertNotNull("Test initialize MetricsConfig instance.", mc);

    Set<Entry<String, String>> sub = mc.subset("test", "source");
    assertEquals("Subset size is as expected.", 2, sub.size());
    shouldReject(sub, "bar");
    shouldReject(sub, tags(newTag("bar", "")));
    shouldReject(sub, tags(newTag("foo", "boo")));
  }

  public void testExcludeAndExcludeTag() throws Exception {
    final Properties prop = new Properties();
    prop.setProperty("p.exclude", "foo");
    prop.setProperty("p.exclude.tags", "foo:f");
    final MetricsConfig mc = new MetricsConfig(prop);
    Set<Entry<String, String>> sub = mc.subset("p");
    shouldAccept(sub, "bar");
    shouldAccept(sub, tags(newTag("bar", "")));
    shouldReject(sub, "foo");
    shouldReject(sub, tags(newTag("bar", ""), newTag("foo", "f")));
  }

  public void testAcceptUnmatchedWhenBothAreConfigured() throws Exception {
    final Properties prop = new Properties();
    prop.setProperty("p.include", "foo");
    prop.setProperty("p.include.tags", "foo:f");
    prop.setProperty("p.exclude", "bar");
    prop.setProperty("p.exclude.tags", "bar:b");
    final MetricsConfig mc = new MetricsConfig(prop);
    Set<Entry<String, String>> sub = mc.subset("p"); 
    shouldAccept(sub, "foo");
    shouldAccept(sub, tags(newTag("foo", "f")));
    shouldReject(sub, "bar");
    shouldReject(sub, tags(newTag("bar", "b")));
    shouldAccept(sub, "foobar");
    shouldAccept(sub, tags(newTag("foobar", "")));
  }

  public void testIncludeOverrideExclude() throws Exception {
    final Properties prop = new Properties();
    prop.setProperty("p.include", "foo");
    prop.setProperty("p.include.tags", "foo:f");
    prop.setProperty("p.exclude", "foo");
    prop.setProperty("p.exclude.tags", "foo:f");
    final MetricsConfig mc = new MetricsConfig(prop);
    Set<Entry<String, String>> sub = mc.subset("p"); 
    shouldAccept(sub, "foo");
    shouldAccept(sub, tags(newTag("foo", "f")));
  }

  static void shouldAccept(Set<Entry<String, String>> sub, String s) {
    assertTrue("Accepts "+ s, newGlobalFilter(sub).accepts(s));
    assertTrue("Accepts "+ s, newRegexFilter(sub).accepts(s));
  }

  static void shouldAccept(Set<Entry<String, String>> sub, 
      Iterable<MetricsTag> tags) {
    assertTrue("Accepts "+ tags, newGlobalFilter(sub).accepts(tags));
    assertTrue("Accepts "+ tags, newRegexFilter(sub).accepts(tags));
  }

  static void shouldReject(Set<Entry<String, String>> sub, String s) {
    assertTrue("Rejects "+ s, !newGlobalFilter(sub).accepts(s));
    assertTrue("Rejects "+ s, !newRegexFilter(sub).accepts(s));
  }

  static void shouldReject(Set<Entry<String, String>> sub, 
      Iterable<MetricsTag> tags) {
    assertTrue("Rejects "+ tags, !newGlobalFilter(sub).accepts(tags));
    assertTrue("Rejects "+ tags, !newRegexFilter(sub).accepts(tags));
  }

  static GlobalFilter newGlobalFilter(Set<Entry<String, String>> sub) {
    return new GlobalFilter(sub);
  }

  static RegexFilter newRegexFilter(Set<Entry<String, String>> sub) {
    return new RegexFilter(sub);
  }

  private Iterable<MetricsTag> tags(MetricsTag ... tags){
    return Arrays.asList(tags);
  }

  private MetricsTag newTag(String pre, String t){
    return new MetricsTag(pre, t);
  }

  public void tearDown() throws Exception { }
}
