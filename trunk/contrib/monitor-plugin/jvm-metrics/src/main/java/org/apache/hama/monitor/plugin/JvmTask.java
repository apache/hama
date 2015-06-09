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
package org.apache.hama.monitor.plugin;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import static java.lang.Thread.State.*;
import static org.apache.hama.monitor.plugin.JvmTask.Metrics.*;
import static org.apache.hama.monitor.Monitor.Destination.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hama.monitor.Metric;
import org.apache.hama.monitor.MetricsRecord; // TODO: should be moved to org.apache.hama.monitor.metrics package
import org.apache.hama.monitor.Monitor.Destination;
import org.apache.hama.monitor.Monitor.Result;
import org.apache.hama.monitor.Monitor.Task;
import org.apache.hama.monitor.Monitor.TaskException;

public final class JvmTask extends Task {

  public static final Log LOG = LogFactory.getLog(JvmTask.class);

  private final MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
  private final List<GarbageCollectorMXBean> gcBeans =
      ManagementFactory.getGarbageCollectorMXBeans();
  private final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
  private static final long M = 1024*1024;
 
  private final Result result;

  public static enum Metrics{
    MemNonHeapUsedM("Non-heap memory used in MB"),
    MemNonHeapCommittedM("Non-heap memory committed in MB"),
    MemHeapUsedM("Heap memory used in MB"),
    MemHeapCommittedM("Heap memory committed in MB"),
    GcCount("Total GC count"),
    GcTimeMillis("Total GC time in milliseconds"),
    ThreadsNew("Number of new threads"),
    ThreadsRunnable("Number of runnable threads"),
    ThreadsBlocked("Number of blocked threads"),
    ThreadsWaiting("Number of waiting threads"),
    ThreadsTimedWaiting("Number of timed waiting threads"),
    ThreadsTerminated("Number of terminated threads");

    private final String desc;

    Metrics(String desc) { this.desc = desc; }
    public String description() { return desc; }

    @Override public String toString(){
      return "name "+name()+" description "+desc;
    }
  }

  public static final class JvmResult implements Result {
    final AtomicReference<MetricsRecord> ref = 
      new AtomicReference<MetricsRecord>();

    @Override
    public String name() {
      //return JvmResult.class.getSimpleName();
      return "jvm"; 
    }

    public void set(MetricsRecord record) {
      ref.set(record);
    }

    @Override
    public MetricsRecord get() {
      return this.ref.get();
    }

    @Override
    public Destination[] destinations() {
      return new Destination[]{ ZK, HDFS, JMX };
    }     

  }

  public JvmTask() {
    super(JvmTask.class.getSimpleName());
    this.result = new JvmResult();
  }

  @Override
  public Object run() throws TaskException {
    final MetricsRecord record = new MetricsRecord("jvm", "Jvm metrics stats.");
    //record.tag("ProcessName", processName);
    //record.tag("SessionId", sessionId);
    memory(record);
    gc(record);
    threads(record);
    ((JvmResult)this.result).set(record); // add to results
    getListener().notify(this.result); // notify monitor
    return null;
  }  

  private void memory(final MetricsRecord record){
    final MemoryUsage memNonHeap = memoryMXBean.getNonHeapMemoryUsage();
    final MemoryUsage memHeap = memoryMXBean.getHeapMemoryUsage();
    record.add(new Metric(MemNonHeapUsedM, memNonHeap.getUsed() / M));
    record.add(new Metric(MemNonHeapCommittedM,
                           memNonHeap.getCommitted() / M));
    record.add(new Metric(MemHeapUsedM, memHeap.getUsed() / M));
    record.add(new Metric(MemHeapCommittedM, memHeap.getCommitted() / M));

    if(LOG.isDebugEnabled()) {
      LOG.debug(MemNonHeapUsedM.description()+": "+memNonHeap.getUsed() / M);
      LOG.debug(MemNonHeapCommittedM.description()+": "+memNonHeap.getCommitted() / M);
      LOG.debug(MemHeapUsedM.description()+": "+memHeap.getUsed() / M);
      LOG.debug(MemHeapCommittedM.description()+": "+memHeap.getCommitted() / M);
    }
  }

  private void gc(final MetricsRecord record){
    long count = 0;
    long timeMillis = 0;
    for (GarbageCollectorMXBean gcBean : gcBeans) {
      long c = gcBean.getCollectionCount();
      long t = gcBean.getCollectionTime();
      String name = gcBean.getName();
      record.add(new Metric("GcCount"+name, c));
      record.add(new Metric("GcTimeMillis"+name, t));
      count += c;
      timeMillis += t;
    }
    record.add(new Metric(GcCount, count));
    record.add(new Metric(GcTimeMillis, timeMillis));

    if(LOG.isDebugEnabled()) {
      LOG.debug(GcCount.description()+": "+count);
      LOG.debug(GcTimeMillis.description()+": "+timeMillis);
    }
  }

  private void threads(final MetricsRecord record){
    int threadsNew = 0;
    int threadsRunnable = 0;
    int threadsBlocked = 0;
    int threadsWaiting = 0;
    int threadsTimedWaiting = 0;
    int threadsTerminated = 0;
    long threadIds[] = threadMXBean.getAllThreadIds();
    for (ThreadInfo threadInfo : threadMXBean.getThreadInfo(threadIds, 0)) {
      if (threadInfo == null) continue; // race protection
      Thread.State state = threadInfo.getThreadState();
      if(NEW.equals(state)){
        threadsNew++;
        break;
      }else if(RUNNABLE.equals(state)){
        threadsRunnable++;
        break;
      }else if(BLOCKED.equals(state)){
        threadsBlocked++;
        break;
      }else if(WAITING.equals(state)){
        threadsWaiting++;
        break;
      }else if(TIMED_WAITING.equals(state)){
        threadsTimedWaiting++;
        break;
      }else if(TERMINATED.equals(state)){
        threadsTerminated++;
        break;
      }
    }

    record.add(new Metric(ThreadsNew, threadsNew));
    record.add(new Metric(ThreadsRunnable, threadsRunnable));
    record.add(new Metric(ThreadsBlocked, threadsBlocked));
    record.add(new Metric(ThreadsWaiting, threadsWaiting));
    record.add(new Metric(ThreadsTimedWaiting, threadsTimedWaiting));
    record.add(new Metric(ThreadsTerminated, threadsTerminated));

    if(LOG.isDebugEnabled()) {
      LOG.debug(ThreadsNew.description()+": "+threadsNew);
      LOG.debug(ThreadsRunnable.description()+": "+threadsRunnable);
      LOG.debug(ThreadsBlocked.description()+": "+threadsBlocked);
      LOG.debug(ThreadsWaiting.description()+": "+threadsWaiting);
      LOG.debug(ThreadsTimedWaiting.description()+": "+threadsTimedWaiting);
      LOG.debug(ThreadsTerminated.description()+": "+threadsTerminated);
    }
  }
}
