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

import static java.util.concurrent.TimeUnit.SECONDS;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hama.Constants;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.HamaTestCase;
import org.apache.hama.bsp.sync.SyncClient;
import org.apache.hama.bsp.sync.SyncException;
import org.apache.hama.bsp.sync.SyncServiceFactory;
import org.apache.hama.ipc.BSPPeerProtocol;
import org.apache.hama.util.BSPNetUtils;

public class TestBSPTaskFaults extends TestCase {

  public static final Log LOG = LogFactory.getLog(HamaTestCase.class);

  public static final String TEST_POINT = "bsp.ft.test.point";
  public static final String TEST_GROOM_PORT = "bsp.ft.test.groomport";
  private static int TEST_NUMBER = 0;
  
  
  private volatile MinimalGroomServer groom;
  private volatile BSPPeerProtocol umbilical;
  private Server workerServer;
  private TaskAttemptID taskid = new TaskAttemptID(new TaskID(new BSPJobID(
      "job_201110302255", 1), 1), 1);

  public volatile HamaConfiguration conf;

  private ScheduledExecutorService testBSPTaskService;
  
  private static synchronized int incrementTestNumber(){
    return ++TEST_NUMBER;
  }

  @SuppressWarnings("unused")
  public static class MinimalGroomServer implements BSPPeerProtocol {

    private volatile int pingCount;
    private volatile long firstPingTime;
    private volatile long lastPingTime;
    private boolean isShutDown = false;
    private boolean taskComplete = false;
    private boolean errorCondition = false;
    private Configuration conf;

    public MinimalGroomServer(Configuration config) throws IOException {
      conf = config;
    }

    @Override
    public long getProtocolVersion(String protocol, long clientVersion)
        throws IOException {
      return BSPPeerProtocol.versionID;
    }

    @Override
    public void close() throws IOException {
      isShutDown = true;

    }

    @Override
    public Task getTask(TaskAttemptID taskid) throws IOException {
      return new BSPTask();
    }

    @Override
    public boolean ping(TaskAttemptID taskid) throws IOException {
      LOG.error("Pinged");
      ++pingCount;
      if (pingCount == 1) {
        firstPingTime = System.currentTimeMillis();
      }
      lastPingTime = System.currentTimeMillis();

      return true;
    }

    @Override
    public void done(TaskAttemptID taskid) throws IOException {
      taskComplete = true;

    }

    @Override
    public void fsError(TaskAttemptID taskId, String message)
        throws IOException {
      errorCondition = true;

    }

    @Override
    public void fatalError(TaskAttemptID taskId, String message)
        throws IOException {
      errorCondition = true;

    }

    @Override
    public boolean statusUpdate(TaskAttemptID taskId, TaskStatus taskStatus)
        throws IOException, InterruptedException {
      return true;
    }

    @Override
    public int getAssignedPortNum(TaskAttemptID taskid) {
      return 0;
    }

    public synchronized int getPingCount() {
      return pingCount;
    }

    public void setPingCount(int pingCount) {
      this.pingCount = pingCount;
      if (pingCount == 0) {
        firstPingTime = 0L;
        lastPingTime = 0L;
      }
    }
  }

  @SuppressWarnings("unused")
  private class TestBSPTaskThreadRunner extends Thread {

    BSPJob job;

    TestBSPTaskThreadRunner(BSPJob jobConf) {
      job = jobConf;
    }

    @SuppressWarnings("rawtypes")
    public void run() {
      BSPTask task = new BSPTask();
      task.setConf(job);

      try {
        BSPPeerImpl<?, ?, ?, ?, ?> bspPeer = new BSPPeerImpl(job, conf, taskid,
            umbilical, 0, null, null, new Counters());
        task.run(job, bspPeer, umbilical); // run the task
      } catch (Exception e) {
        LOG.error("Error in BSPTask execution.", e);
      }

    }
  }

  /*
   * Test BSP exiting its own process. Hence we need a minimal process runner.
   */

  public static class TestBSPProcessRunner implements Callable<Integer> {
    private final ScheduledExecutorService sched;
    private final AtomicReference<ScheduledFuture<Integer>> future;
    private Process bspTaskProcess;
    private Thread errorLog;
    private Thread infoLog;
    private int testPoint;
    private int testPort;

    TestBSPProcessRunner(int point, int port) {
      sched = Executors.newScheduledThreadPool(1);
      future = new AtomicReference<ScheduledFuture<Integer>>();
      bspTaskProcess = null;
      testPoint = point;
      testPort = port;
    }

    private void readStream(InputStream input) throws IOException {
      BufferedReader reader = new BufferedReader(new InputStreamReader(input));
      String line;
      while ((line = reader.readLine()) != null) {
        LOG.info(line);
      }
    }

    public void startBSPProcess() {
      this.future.set(this.sched.schedule(this, 0, SECONDS));
      LOG.debug("Start building BSPPeer process.");
    }

    public int getBSPExitCode() {
      try {
        return this.future.get().get();
      } catch (Exception e) {
        LOG.error("Error while fetching exit status from BSPTask", e);
      } finally {

      }
      return -1;
    }

    public void destroyProcess() {

    }

    @Override
    public Integer call() throws Exception {
      String SYSTEM_PATH_SEPARATOR = System.getProperty("path.separator");
      List<String> commands = new ArrayList<String>();
      String workDir = new File(".").getAbsolutePath();
      File jvm = // use same jvm as parent
      new File(new File(System.getProperty("java.home"), "bin"), "java");
      commands.add(jvm.toString());

      StringBuffer classPath = new StringBuffer();
      // start with same classpath as parent process
      classPath.append(System.getProperty("java.class.path"));
      classPath.append(SYSTEM_PATH_SEPARATOR);
      classPath.append(new File(workDir, "core/target/test-classes"));
      classPath.append(SYSTEM_PATH_SEPARATOR);
      classPath.append(workDir);

      commands.add("-classpath");
      commands.add(classPath.toString());

      commands.add(TestBSPProcessRunner.class.getName());

      LOG.info("starting process for failure case - "
          + testPoint);
      commands.add("" + testPoint);
      commands.add("" + testPort);

      LOG.info(commands.toString());

      ProcessBuilder builder = new ProcessBuilder(commands);

      try {
        bspTaskProcess = builder.start();

        // We have errorLog and infoLog to prevent block on pipe between
        // child and parent process.
        errorLog = new Thread() {
          public void run() {
            try {
              readStream(bspTaskProcess.getErrorStream());
            } catch (Exception e) {

            }
          }
        };
        errorLog.start();

        infoLog = new Thread() {
          public void run() {
            try {
              readStream(bspTaskProcess.getInputStream());
            } catch (Exception e) {

            }
          }
        };
        infoLog.start();

        int exit_code = bspTaskProcess.waitFor();
        return exit_code;
      } catch (Exception e) {
        LOG.error("Error getting exit code of child process", e);
      }
      return -1;
    }

    public static void main(String[] args) {

      HamaConfiguration hamaConf = new HamaConfiguration();
      hamaConf.setInt(Constants.GROOM_PING_PERIOD, 200);
      hamaConf.setClass("bsp.work.class", FaulTestBSP.class, BSP.class);
      hamaConf.setClass(SyncServiceFactory.SYNC_CLIENT_CLASS,
          LocalBSPRunner.LocalSyncClient.class, SyncClient.class);

      hamaConf.setInt("bsp.master.port", 610002);

      TaskAttemptID tid = new TaskAttemptID(new TaskID(new BSPJobID(
          "job_201110102255", 1), 1), 1);

      hamaConf.setInt(TEST_POINT, Integer.parseInt(args[0]));
      int port = Integer.parseInt(args[1]);

      try {
        BSPJob job = new BSPJob(hamaConf);
        final BSPPeerProtocol proto = (BSPPeerProtocol) RPC.getProxy(
            BSPPeerProtocol.class, BSPPeerProtocol.versionID,
            new InetSocketAddress("127.0.0.1", port), hamaConf);

        BSPTask task = new BSPTask();
        task.setConf(job);

        LOG.info("Testing failure case in process - "
            + hamaConf.getInt(TEST_POINT, 0));

        Runtime.getRuntime().addShutdownHook(new Thread() {
          public void run() {
            try {
              proto.close();
            } catch (Exception e) {
              // too late to log!
            }
          }
        });

        @SuppressWarnings("rawtypes")
        BSPPeerImpl<?, ?, ?, ?, ?> bspPeer = new BSPPeerImpl(job, hamaConf,
            tid, proto, 0, null, null, new Counters());
        task.run(job, bspPeer, proto); // run the task

      } catch (Exception e) {
        LOG.error("Error in bsp child process.", e);
      }

    }

  }

  /*
   * Test BSP class that has faults injected in each phase.
   */
  private static class FaulTestBSP extends
      BSP<NullWritable, NullWritable, NullWritable, NullWritable, NullWritable> {

    @Override
    public void setup(
        BSPPeer<NullWritable, NullWritable, NullWritable, NullWritable, NullWritable> peer)
        throws IOException, SyncException, InterruptedException {
      if (peer.getConfiguration().getInt(TEST_POINT, 0) == 1) {
        throw new RuntimeException("Error injected in setup");
      }
      Thread.sleep(500);
      super.setup(peer);
      LOG.info("Succesfully completed setup for bsp.");
    }

    @Override
    public void cleanup(
        BSPPeer<NullWritable, NullWritable, NullWritable, NullWritable, NullWritable> peer)
        throws IOException {
      if (peer.getConfiguration().getInt(TEST_POINT, 0) == 3) {
        throw new RuntimeException("Error injected in cleanup");
      }
      try {
        Thread.sleep(500);
      } catch (Exception e) {
        LOG.error("Interrupted BSP thread.", e);
      }
      super.cleanup(peer);
      LOG.info("Succesfully cleaned up after bsp.");
    }

    @Override
    public void bsp(
        BSPPeer<NullWritable, NullWritable, NullWritable, NullWritable, NullWritable> peer)
        throws IOException, SyncException, InterruptedException {
      if (peer.getConfiguration().getInt(TEST_POINT, 0) == 2) {
        throw new RuntimeException("Error injected in bsp function");
      }
      Thread.sleep(500);
      LOG.info("Succesfully completed bsp.");
    }

  }

  @Override
  protected void setUp() throws Exception {

    super.setUp();
    conf = new HamaConfiguration();

    conf.setInt(Constants.GROOM_PING_PERIOD, 200);
    conf.setClass("bsp.work.class", FaulTestBSP.class, BSP.class);
    conf.setClass(SyncServiceFactory.SYNC_CLIENT_CLASS,
        LocalBSPRunner.LocalSyncClient.class, SyncClient.class);

    int testNumber = incrementTestNumber();
    InetSocketAddress inetAddress = new InetSocketAddress(BSPNetUtils.getFreePort(34321) + testNumber);
    groom = new MinimalGroomServer(conf);
    workerServer = RPC.getServer(groom, inetAddress.getHostName(),
        inetAddress.getPort(), conf);
    workerServer.start();

    LOG.info("Started RPC server");
    conf.setInt("bsp.groom.rpc.port", inetAddress.getPort());

    umbilical = (BSPPeerProtocol) RPC.getProxy(BSPPeerProtocol.class,
        BSPPeerProtocol.versionID, inetAddress, conf);
    LOG.info("Started the proxy connections");

    this.testBSPTaskService = Executors.newScheduledThreadPool(1);
  }

  private int getExpectedPingCounts() {
    return ((int) (2 * (groom.lastPingTime - groom.firstPingTime) / (conf
        .getInt(Constants.GROOM_PING_PERIOD, 5000))));
  }

  private void checkIfPingTestPassed() {
    int expectedPingCounts = getExpectedPingCounts();
    LOG.info("Counted " + groom.pingCount + " pings and expected "
        + expectedPingCounts + " pings.");
    boolean testPass = groom.getPingCount() >= expectedPingCounts;
    assertEquals(true, testPass);
  }

  /*
   * Test if we get the expected counts of ping.
   */
  public void testPing() {
    conf.setInt(TEST_POINT, 0);

    CompletionService<Integer> completionService = new ExecutorCompletionService<Integer>(
        this.testBSPTaskService);
    Future<Integer> future = completionService
        .submit(new TestBSPProcessRunner(0, workerServer.getListenerAddress().getPort()));

    try {
      future.get(20000, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e1) {
      LOG.error("Interrupted Exception.", e1);
    } catch (ExecutionException e1) {
      LOG.error("ExecutionException Exception.", e1);
    } catch (TimeoutException e) {
      LOG.error("TimeoutException Exception.", e);
    }

    checkIfPingTestPassed();
    groom.setPingCount(0);
    this.testBSPTaskService.shutdownNow();

  }

  /*
   * Inject failure and different points and sense if the pings are coming or
   * not.
   */
  public void testPingOnTaskSetupFailure() {

    LOG.info("Testing ping failure case - 1");
    conf.setInt(TEST_POINT, 1);

    CompletionService<Integer> completionService = 
        new ExecutorCompletionService<Integer>(this.testBSPTaskService);
    Future<Integer> future = completionService
        .submit(new TestBSPProcessRunner(1, 
            workerServer.getListenerAddress().getPort()));

    try {
      future.get(20000, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e1) {
      LOG.error("Interrupted Exception.", e1);
    } catch (ExecutionException e1) {
      LOG.error("ExecutionException Exception.", e1);
    } catch (TimeoutException e) {
      LOG.error("TimeoutException Exception.", e);
    }

    checkIfPingTestPassed();
    groom.setPingCount(0);
    this.testBSPTaskService.shutdownNow();

  }

  public void testPingOnTaskExecFailure() {

    LOG.info("Testing ping failure case - 2");
    conf.setInt(TEST_POINT, 2);
    CompletionService<Integer> completionService = 
        new ExecutorCompletionService<Integer>(this.testBSPTaskService);
    Future<Integer> future = completionService
        .submit(new TestBSPProcessRunner(2, 
            workerServer.getListenerAddress().getPort()));

    try {
      future.get(20000, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e1) {
      LOG.error("Interrupted Exception.", e1);
    } catch (ExecutionException e1) {
      LOG.error("ExecutionException Exception.", e1);
    } catch (TimeoutException e) {
      LOG.error("TimeoutException Exception.", e);
    }

    checkIfPingTestPassed();
    groom.setPingCount(0);
    this.testBSPTaskService.shutdownNow();

  }

  public void testPingOnTaskCleanupFailure() {

    LOG.info("Testing ping failure case - 3");

    conf.setInt(TEST_POINT, 3);
    CompletionService<Integer> completionService = 
        new ExecutorCompletionService<Integer>(this.testBSPTaskService);
    
    Future<Integer> future = completionService
        .submit(new TestBSPProcessRunner(3, 
            workerServer.getListenerAddress().getPort()));

    try {
      future.get(20000, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e1) {
      LOG.error("Interrupted Exception.", e1);
    } catch (ExecutionException e1) {
      LOG.error("ExecutionException Exception.", e1);
    } catch (TimeoutException e) {
      LOG.error("TimeoutException Exception.", e);
    }

    checkIfPingTestPassed();
    groom.setPingCount(0);
    this.testBSPTaskService.shutdownNow();

  }

  public void testBSPTaskSelfDestroy() {
    LOG.info("Testing self kill on lost contact.");

    CompletionService<Integer> completionService = 
        new ExecutorCompletionService<Integer>(this.testBSPTaskService);
    Future<Integer> future = completionService
        .submit(new TestBSPProcessRunner(0, 
            workerServer.getListenerAddress().getPort()));

    try {
      while (groom.pingCount == 0) {
        Thread.sleep(100);
      }
    } catch (Exception e) {
      LOG.error("Interrupted the timer for 1 sec.", e);
    }

    workerServer.stop();
    umbilical = null;
    workerServer = null;
    Integer exitValue = -1;
    try {
      exitValue = future.get(20000, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e1) {
      LOG.error("Interrupted Exception.", e1);
    } catch (ExecutionException e1) {
      LOG.error("ExecutionException Exception.", e1);
    } catch (TimeoutException e) {
      LOG.error("TimeoutException Exception.", e);
    }

    assertEquals(69, exitValue.intValue());
  }

  @Override
  protected void tearDown() throws Exception {

    super.tearDown();
    if (groom != null)
      groom.setPingCount(0);
    if (umbilical != null) {
      umbilical.close();
      Thread.sleep(2000);
    }
    if (workerServer != null)
      workerServer.stop();
    testBSPTaskService.shutdownNow();
    Thread.sleep(2000);
  }

}
