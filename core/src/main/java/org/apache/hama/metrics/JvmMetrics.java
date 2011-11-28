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

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.List;
import static java.lang.Thread.State.*;
import static org.apache.hama.metrics.JvmMetrics.Metrics.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hama.HamaConfiguration;

public final class JvmMetrics  implements MetricsSource {

  public static final Log LOG = LogFactory.getLog(JvmMetrics.class);

  private final MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
  private final List<GarbageCollectorMXBean> gcBeans =
      ManagementFactory.getGarbageCollectorMXBeans();
  private final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
  private static final long M = 1024*1024;
  private final String name, description, processName, sessionId;

  public enum Metrics{ 
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

  public JvmMetrics(String processName, String sessionId, HamaConfiguration conf) {
    this.name = "JvmMetric";
    this.description = "JVM metrics information.";
    this.processName = processName;
    this.sessionId = sessionId;
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
     final MetricsRecord record = new MetricsRecord(name, description);
     record.tag("ProcessName", processName);
     record.tag("SessionId", sessionId);
     memory(record);
     gc(record);
     threads(record);
     collector.add(record);
  }

  private void memory(final MetricsRecord record){
    MemoryUsage memNonHeap = memoryMXBean.getNonHeapMemoryUsage();
    MemoryUsage memHeap = memoryMXBean.getHeapMemoryUsage();
    record.add(new Metric(MemNonHeapUsedM, memNonHeap.getUsed() / M));
    record.add(new Metric(MemNonHeapCommittedM, 
                           memNonHeap.getCommitted() / M));
    record.add(new Metric(MemHeapUsedM, memHeap.getUsed() / M));
    record.add(new Metric(MemHeapCommittedM, memHeap.getCommitted() / M));
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
  }
}
