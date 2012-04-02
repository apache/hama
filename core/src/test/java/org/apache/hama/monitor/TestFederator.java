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
package org.apache.hama.monitor;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import junit.framework.TestCase;

import static org.junit.Assert.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hama.HamaConfiguration;
import org.apache.hama.monitor.Federator.Act;
import org.apache.hama.monitor.Federator.Collector;
import org.apache.hama.monitor.Federator.CollectorHandler;

public class TestFederator extends TestCase {

  public static final Log LOG = LogFactory.getLog(TestFederator.class);

  Federator federator; 

  final static int expected = 10;

  public static final class DummyCollector implements Collector {

    final AtomicInteger sum = new AtomicInteger(0);

    public DummyCollector(int value) {
      sum.set(value);
    } 

    public Object harvest() throws Exception {
      assertEquals("Test if value is equal before harvest.", expected, sum.get()); 
      int result = sum.incrementAndGet(); 
      Thread.sleep(2*1000); // simulate task execution which takes time.
      assertEquals("Test if value is equal after harvest.", (expected+1), result); 
      return result;
    }

  }

  public void setUp() throws Exception {
    this.federator = new Federator(new HamaConfiguration());
    this.federator.start();
  }

  public void testExecutionFlow() throws Exception {
    LOG.info("Value before submitted: "+expected);
    final AtomicInteger finalResult = new AtomicInteger(0);
    final Act act = new Act(new DummyCollector(expected), new CollectorHandler() {
      public void handle(Future future) {
        try {
          finalResult.set(((Integer)future.get()).intValue());
          LOG.info("Value after submitted: "+finalResult);
        } catch (ExecutionException ee) {
          LOG.error(ee);
        } catch (InterruptedException ie) {
          LOG.error(ie);
          Thread.currentThread().interrupt();
        }
      }
    });
    this.federator.register(act);
    Thread.sleep(3*1000);
    assertEquals("Result should be "+(expected+1)+".", finalResult.get(), (expected+1)); 
  }
  
  public void tearDown() throws Exception {
    this.federator.interrupt();
  }
  
}
