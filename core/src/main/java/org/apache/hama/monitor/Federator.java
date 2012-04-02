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

import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hama.HamaConfiguration;

public final class Federator extends Thread {

  public static final Log LOG = LogFactory.getLog(Federator.class);
 
  private final HamaConfiguration configuration;

  private final ExecutorService workers;

  private final BlockingQueue<Act> commands = new LinkedBlockingQueue<Act>();

  private static class ServiceWorker implements Callable {

    final Collector collector;

    public ServiceWorker(final Collector collector) {
      this.collector = collector;
    }

    @Override
    public Object call() throws Exception {
      return this.collector.harvest();
    }

  }
 
  /**
   * A token binds collector and handler together.
   */
  public static final class Act {
    final Collector collector;
    final CollectorHandler handler;

    public Act(final Collector collector, final CollectorHandler handler) {
      this.collector = collector;
      this.handler = handler;
    }

    public final Collector collector() {
      return this.collector;
    }

    public final CollectorHandler handler() {
      return this.handler;
    }

  }

  /**
   * Purpose to handle the result returned by Collector.
   */
  public static interface CollectorHandler {

    /**
     * Handle the result.
     */
    void handle(Future future); 

  }
  
  /**
   * Collect GroomServer information from repository. 
   */
  public static interface Collector {

    /**
     * Function is called to collect GroomServer information from specific
     * place.
     */
    Object harvest() throws Exception;

  }

  public Federator(final HamaConfiguration configuration) {
    this.configuration = configuration;
    this.workers = Executors.newCachedThreadPool();
    setName(Federator.class.getSimpleName());
    setDaemon(true);
  }

  public final void register(final Act act) {
    try {
      if(null == act || null == act.collector() || null == act.handler())
        throw new NullPointerException("Collector or CollectorHandler "+
        " is not provided."); 
      commands.put(act);
    } catch (InterruptedException ie) {
      LOG.error(ie);
      Thread.currentThread().interrupt();
    }
  }

  public void run() {
    try {
      while(!Thread.currentThread().interrupted()) {
        Act act = commands.take();
        act.handler().handle(workers.submit(new ServiceWorker(act.collector())));
      }
    } catch (InterruptedException ie) {
      LOG.error(ie);
      Thread.currentThread().interrupt();
    }
  }

}
