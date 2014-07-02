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
package org.apache.hama.bsp;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hama.bsp.TaskWorkerManager.TaskWorker;
import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.Protos.CommandInfo;
import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.Value;
import org.apache.hadoop.conf.Configuration;

import com.google.protobuf.ByteString;

public class ResourceManager {
  public static final Log log = LogFactory.getLog(ResourceManager.class);

  private final String anyGroomServer = "_any_";

  private Configuration conf;
  private static long launchedTasks = 0;

  private Set<JobInProgress> executingJobs =  Collections.synchronizedSet(new HashSet<JobInProgress>());
  private Set<TaskInProgress> executingTasks = Collections.synchronizedSet(new HashSet<TaskInProgress>());
  private Map<String, java.util.Queue<TaskInProgress>> tasksToRunByGroom;
  private Set<TaskInProgress> tasksToRun;

  private Set<TaskInProgress> recoveryTasks = Collections.synchronizedSet(new HashSet<TaskInProgress>());

  private long slotMemory;
  // Overhead requirements for the container groom server
  double groomCpus;
  double groomMem;
  double groomDisk;

  TaskDelegator taskDelegator;

  /**
   * Constructor for the mesos resource manager
   * 
   * @param conf The configuration options for hama
   * @param serverManager A reference to the groom server manager
   * @param driver The mesos driver. This is required to terminate tasks
   */
  public ResourceManager(Configuration conf,
      AtomicReference<GroomServerManager> serverManager, SchedulerDriver driver) {
    tasksToRunByGroom = new ConcurrentHashMap<String, java.util.Queue<TaskInProgress>>();
    tasksToRunByGroom.put(anyGroomServer, new ConcurrentLinkedQueue<TaskInProgress>());
    tasksToRun = new HashSet<TaskInProgress>();

    slotMemory = parseMemory(conf);

    taskDelegator = new TaskDelegator(serverManager, driver, recoveryTasks);
    serverManager.get().addGroomStatusListener(taskDelegator);
    this.conf = conf;

    groomCpus = conf.getInt("hama.mesos.groom.cpu", 0);
    groomMem = conf.getInt("hama.mesos.groom.mem", 200);
    groomDisk = conf.getInt("hama.mesos.groom.disk", 0);
  }

  /**
   * Handle a resource offer by the mesos framework
   * 
   * @param schedulerDriver The mesos scheduler driver
   * @param offers A list of offers from mesos
   */
  public void resourceOffers(SchedulerDriver schedulerDriver, List<Offer> offers) {

    if (tasksToRun.isEmpty()) {
      // there is no need to track executing tasks if everything is
      // started
      clearQueues();

      for (Offer offer : offers) {
        schedulerDriver.declineOffer(offer.getId());
      }
    } else {
      for (Offer offer : offers) {
        useOffer(schedulerDriver, offer);
      }
    }
  }

  private void clearQueues() {
    synchronized (tasksToRunByGroom) {
      for (java.util.Queue<TaskInProgress> queue : tasksToRunByGroom.values()) {
        queue.clear();
      }
      executingTasks.clear();
    }
  }

  private void useOffer(SchedulerDriver schedulerDriver, Offer offer) {
    log.debug("Received offer From: " + offer.getHostname());

    String host = offer.getHostname();

    ResourceOffer ro = new ResourceOffer(offer);
    int maxSlots = ro.getMaxSlots();
    if (maxSlots == 0) {
      schedulerDriver.declineOffer(offer.getId());
      return;
    }

    java.util.Queue<TaskInProgress> tasks = new LinkedList<TaskInProgress>();

    while (tasks.size() < maxSlots) {
      TaskInProgress tip = null;
      if (tasksToRunByGroom.get(host) != null) {
        tip = tasksToRunByGroom.get(host).poll();
      }

      if (tip == null) {
        tip = tasksToRunByGroom.get(anyGroomServer).poll();
        if (tip == null) {
          if (tasks.isEmpty()) {
            schedulerDriver.declineOffer(offer.getId());
          }

          break;
        }
      }
      if (executingTasks.contains(tip)) {
        continue;
      }

      executingTasks.add(tip);
      tasksToRun.remove(tip);

      tasks.add(tip);

      log.debug("Found offer for: " + tip.getTaskId());
    }

    if (!tasks.isEmpty()) {
      launchTasks(schedulerDriver, tasks, ro);
    }
  }

  class MesosTaskWorker implements TaskWorker {
    private final JobInProgress jip;

    public MesosTaskWorker(JobInProgress jip) {
      this.jip = jip;
    }

    @Override
    public Boolean call() throws Exception {
      log.debug("Task Worker called: " + jip.tasks.length);

      for (TaskInProgress tip : jip.tasks) {
        if (jip.isRecoveryPending()) {
          recoveryTasks.add(tip);
        }
        String[] grooms = jip.getPreferredGrooms(tip, null, null);

        if (grooms == null) {
          grooms = new String[] { anyGroomServer };
        }
        log.info("Prefered Groom for tip " + tip.idWithinJob() + ": "
            + grooms[0]);
        synchronized (tasksToRunByGroom) {
          for (String groom : grooms) {
            if (!tasksToRunByGroom.containsKey(groom)) {
              tasksToRunByGroom.put(groom, new ConcurrentLinkedQueue<TaskInProgress>());
              log.info("Received request for groom: " + groom);
            }
            tasksToRunByGroom.get(groom).add(tip);
          }
          tasksToRun.add(tip);
        }
      }

      executingJobs.add(jip);
      return true;
    }
  }

  private void launchTasks(SchedulerDriver schedulerDriver,
      java.util.Queue<TaskInProgress> tips, ResourceOffer offer) {
    TaskID taskId = TaskID.newBuilder().setValue("Task_" + launchedTasks++)
        .build();

    List<Long> ports = claimPorts(offer.ports, 2);

    double taskCpus = 1 * tips.size() + groomCpus;
    double taskMem = slotMemory * tips.size() + groomMem;
    double taskDisk = 10 + groomDisk;

    String uri = conf.get("hama.mesos.executor.uri");
    if (uri == null) {
      throw new RuntimeException(
          "Expecting configuration property 'mapred.mesos.executor.uri'");
    }

    String directory = conf.get("hama.mesos.executor.directory");
    if (directory == null || directory.equals("")) {
      log.info("URI: " + uri + ", name: " + new File(uri).getName());

      directory = new File(uri).getName().split("\\.")[0] + "*";
    }
    log.debug("Directory: " + directory);
    String command = conf.get("hama.mesos.executor.command");
    if (command == null || command.equals("")) {
      command = "env ; bash -x ./bin/hama org.apache.hama.bsp.MesosExecutor";
    }

    // Set up the environment for running the TaskTracker.
    Protos.Environment.Builder envBuilder = Protos.Environment.newBuilder();

    // Set java specific environment, appropriately.
    Map<String, String> env = System.getenv();
    if (env.containsKey("JAVA_HOME")) {
      envBuilder.addVariables(Protos.Environment.Variable.newBuilder()
          .setName("JAVA_HOME").setValue(env.get("JAVA_HOME")));
    }

    envBuilder.addVariables(Protos.Environment.Variable.newBuilder()
        .setName("HAMA_LOG_DIR").setValue("logs"));

    log.debug("JAVA_HOME: " + env.get("JAVA_HOME"));
    if (env.containsKey("JAVA_LIBRARY_PATH")) {
      envBuilder.addVariables(Protos.Environment.Variable.newBuilder()
          .setName("JAVA_LIBRARY_PATH").setValue(env.get("JAVA_LIBRARY_PATH")));
    }
    log.debug("JAVA_LIBRARY_PATH: " + env.get("JAVA_LIBRARY_PATH"));

    CommandInfo commandInfo = CommandInfo.newBuilder()
        .setEnvironment(envBuilder)
        .setValue(String.format("cd %s && %s", directory, command))
        .addUris(CommandInfo.URI.newBuilder().setValue(uri)).build();

    log.debug("Offer: cpus:  " + offer.cpus + " mem: " + offer.mem + "disk: "
        + offer.disk);
    log.debug("Cpu: " + taskCpus + " Mem: " + taskMem + " Disk: " + taskDisk
        + " port: " + ports.get(0));
    TaskInfo info = TaskInfo
        .newBuilder()
        .setName(taskId.getValue())
        .setTaskId(taskId)
        .setSlaveId(offer.offer.getSlaveId())
        .addResources(
            Resource.newBuilder().setName("cpus").setType(Value.Type.SCALAR)
                .setRole(offer.cpuRole)
                .setScalar(Value.Scalar.newBuilder().setValue(taskCpus)))
        .addResources(
            Resource.newBuilder().setName("mem").setType(Value.Type.SCALAR)
                .setRole(offer.memRole)
                .setScalar(Value.Scalar.newBuilder().setValue(taskMem)))
        .addResources(
            Resource.newBuilder().setName("disk").setType(Value.Type.SCALAR)
                .setRole(offer.diskRole)
                .setScalar(Value.Scalar.newBuilder().setValue(taskDisk)))
        .addResources(
            Resource
                .newBuilder()
                .setName("ports")
                .setType(Value.Type.RANGES)
                .setRole(offer.portRole)
                .setRanges(
                    Value.Ranges
                        .newBuilder()
                        .addRange(
                            Value.Range.newBuilder().setBegin(ports.get(0))
                                .setEnd(ports.get(0)))
                        .addRange(
                            Value.Range.newBuilder().setBegin(ports.get(1))
                                .setEnd(ports.get(1)))))
        .setExecutor(
            ExecutorInfo
                .newBuilder()
                .setExecutorId(
                    ExecutorID.newBuilder().setValue(
                        "executor_" + taskId.getValue()))
                .setName("Hama Groom Server").setSource(taskId.getValue())
                .setCommand(commandInfo))
        .setData(
            ByteString.copyFrom(getConfigurationOverride(ports.get(0),
                ports.get(1), tips.size(), (long) taskMem))).build();

    log.debug("Accepting offer " + offer.offer.getId() + " cpus: " + taskCpus
        + " mem: " + taskMem);
    for (TaskInProgress tip : tips) {
      taskDelegator.addTask(tip, taskId, offer.offer.getHostname(), ports
          .get(0).intValue());
    }
    schedulerDriver.launchTasks(offer.offer.getId(), Arrays.asList(info));
  }

  private List<Long> claimPorts(List<Value.Range> offeredPorts, int count) {
    List<Long> ports = new ArrayList<Long>(count);

    for (Value.Range range : offeredPorts) {
      long begin = range.getBegin();

      while (ports.size() < count & range.getEnd() != begin) {
        ports.add(begin++);
      }

      if (ports.size() == count) {
        break;
      }
    }

    return ports;
  }

  private byte[] getConfigurationOverride(Long groomRPCPort,
      Long groomPeerPort, Integer maxTasks, Long slotJVMHeap) {
    // Create a configuration from the current configuration and
    // override properties as appropriate for the Groom server.
    Configuration overrides = new Configuration(conf);

    overrides.set("bsp.groom.rpc.port", groomRPCPort.toString());
    overrides.set("bsp.peer.port", groomPeerPort.toString());
    overrides.set("bsp.tasks.maximum", maxTasks.toString());

    overrides.set("bsp.child.java.opts", conf.get("bsp.child.java.opts")
        + slotJVMHeap + "m");

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      overrides.write(new DataOutputStream(baos));
      baos.flush();
    } catch (IOException e) {
      log.warn("Failed to serialize configuration.", e);
      System.exit(1);
    }

    return baos.toByteArray();
  }

  private class ResourceOffer {
    Offer offer;

    double cpus;
    double mem;
    double disk;
    List<Value.Range> ports;

    String cpuRole;
    String memRole;
    String diskRole;
    String portRole;

    public ResourceOffer(Offer offer) {
      this.offer = offer;
      parseOffer(offer);
    }

    private void parseOffer(Offer offer) {
      // Pull out the cpus, memory, disk, and 2 ports from the offer.
      for (Resource resource : offer.getResourcesList()) {
        if (resource.getName().equals("cpus")
            && resource.getType() == Value.Type.SCALAR) {
          cpus = resource.getScalar().getValue();
          cpuRole = resource.getRole();
        } else if (resource.getName().equals("mem")
            && resource.getType() == Value.Type.SCALAR) {
          mem = resource.getScalar().getValue();
          memRole = resource.getRole();
        } else if (resource.getName().equals("disk")
            && resource.getType() == Value.Type.SCALAR) {
          disk = resource.getScalar().getValue();
          diskRole = resource.getRole();
        } else if (resource.getName().equals("ports")
            && resource.getType() == Value.Type.RANGES) {
          portRole = resource.getRole();
          ports = resource.getRanges().getRangeList();
        }
      }
    }

    private int getMaxSlots() {
      return (int) Math.min(cpus, mem / slotMemory);
    }
  }

  /**
   * Get the amount of memory requested in MiB
   * 
   * @param javaOpts java options
   * @return mesos formated memory argument
   */
  private static long parseMemory(Configuration conf) {
    String javaOpts = conf.get("bsp.child.java.opts", "-Xmx200m");
    Matcher memMatcher = Pattern.compile("^*-Xmx+([0-9]+)([k,m,g]).*").matcher(
        javaOpts);
    if (memMatcher.matches()) {
      long value = Long.parseLong(memMatcher.group(1));
      String unit = memMatcher.group(2);

      if (unit.equals("k")) {
        value = (long) Math.ceil((float) value / 1024);
      } else if (unit.equals("g")) {
        value = value * 1024;
      }

      // remove memory request from the child java opts so it may be added
      // later
      conf.set("bsp.child.java.opts", memMatcher.replaceAll(""));

      return value;
    } else {
      // default to 200 MiB
      return 200;
    }
  }
}
