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

import java.io.IOException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Enumeration;

import org.apache.hadoop.fs.Path;
import org.apache.hama.HamaConfiguration;

/**
 * A BSP job configuration.
 * 
 * BSPJob is the primary interface for a user to describe a BSP job to the Hama
 * BSP framework for execution.
 */
public class BSPJob extends BSPJobContext {
  public static enum JobState {
    DEFINE, RUNNING
  };

  private JobState state = JobState.DEFINE;
  private BSPJobClient jobClient;
  private RunningJob info;

  public BSPJob() throws IOException {
    this(new HamaConfiguration());
  }

  public BSPJob(HamaConfiguration conf) throws IOException {
    super(conf, null);
    jobClient = new BSPJobClient(conf);
  }

  public BSPJob(HamaConfiguration conf, String jobName) throws IOException {
    this(conf);
    setJobName(jobName);
  }

  public BSPJob(BSPJobID jobID, String jobFile) throws IOException {
    super(new Path(jobFile), jobID);
  }

  public BSPJob(HamaConfiguration conf, Class<?> exampleClass)
      throws IOException {
    this(conf);
    setJarByClass(exampleClass);
  }

  public BSPJob(HamaConfiguration conf, int numPeer) {
    super(conf, null);
    this.setNumBspTask(numPeer);
  }

  private void ensureState(JobState state) throws IllegalStateException {
    if (state != this.state) {
      throw new IllegalStateException("Job in state " + this.state
          + " instead of " + state);
    }
  }

  // /////////////////////////////////////
  // Setter for Job Submission
  // /////////////////////////////////////
  public void setWorkingDirectory(Path dir) throws IOException {
    ensureState(JobState.DEFINE);
    dir = new Path(getWorkingDirectory(), dir);
    conf.set(WORKING_DIR, dir.toString());
  }

  /**
   * Set the BSP algorithm class for the job.
   * 
   * @param cls
   * @throws IllegalStateException
   */
  public void setBspClass(Class<? extends BSP> cls)
      throws IllegalStateException {
    ensureState(JobState.DEFINE);
    conf.setClass(WORK_CLASS_ATTR, cls, BSP.class);
  }

  @SuppressWarnings("unchecked")
  public Class<? extends BSP> getBspClass() {
    return (Class<? extends BSP>) conf.getClass(WORK_CLASS_ATTR, BSP.class);
  }

  public void setJar(String jar) {
    conf.set("bsp.jar", jar);
  }

  public void setJarByClass(Class<?> cls) {
    String jar = findContainingJar(cls);
    if (jar != null) {
      conf.set("bsp.jar", jar);
    }
  }

  private static String findContainingJar(Class<?> my_class) {
    ClassLoader loader = my_class.getClassLoader();
    String class_file = my_class.getName().replaceAll("\\.", "/") + ".class";
    try {
      for (Enumeration<URL> itr = loader.getResources(class_file); itr
          .hasMoreElements();) {

        URL url = itr.nextElement();
        if ("jar".equals(url.getProtocol())) {
          String toReturn = url.getPath();
          if (toReturn.startsWith("file:")) {
            toReturn = toReturn.substring("file:".length());
          }
          toReturn = URLDecoder.decode(toReturn, "UTF-8");
          return toReturn.replaceAll("!.*$", "");
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return null;
  }

  public void setJobName(String name) throws IllegalStateException {
    ensureState(JobState.DEFINE);
    conf.set("bsp.job.name", name);
  }

  public void setInputPath(HamaConfiguration conf, Path iNPUTPATH) {

  }

  public void setUser(String user) {
    conf.set("user.name", user);
  }

  // /////////////////////////////////////
  // Methods for Job Control
  // /////////////////////////////////////
  public long progress() throws IOException {
    ensureState(JobState.RUNNING);
    return info.progress();
  }

  public boolean isComplete() throws IOException {
    ensureState(JobState.RUNNING);
    return info.isComplete();
  }

  public boolean isSuccessful() throws IOException {
    ensureState(JobState.RUNNING);
    return info.isSuccessful();
  }

  public void killJob() throws IOException {
    ensureState(JobState.RUNNING);
    info.killJob();
  }

  public void killTask(TaskAttemptID taskId) throws IOException {
    ensureState(JobState.RUNNING);
    info.killTask(taskId, false);
  }

  public void failTask(TaskAttemptID taskId) throws IOException {
    ensureState(JobState.RUNNING);
    info.killTask(taskId, true);
  }

  public void submit() throws IOException, InterruptedException {
    ensureState(JobState.DEFINE);
    info = jobClient.submitJobInternal(this);
    state = JobState.RUNNING;
  }

  public boolean waitForCompletion(boolean verbose) throws IOException,
      InterruptedException, ClassNotFoundException {
    if (state == JobState.DEFINE) {
      submit();
    }
    if (verbose) {
      jobClient.monitorAndPrintJob(this, info);
    } else {
      info.waitForCompletion();
    }
    return isSuccessful();
  }

  public void set(String name, String value) {
    conf.set(name, value);
  }

  public void setNumBspTask(int tasks) {
    conf.setInt("bsp.peers.num", tasks);
  }

  public int getNumBspTask() {
    return conf.getInt("bsp.peers.num", 0);
  }
}
