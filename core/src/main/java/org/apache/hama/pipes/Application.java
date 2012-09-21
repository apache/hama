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
/** MODIFIED FOR GPGPU Usage! **/

package org.apache.hama.pipes;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.StringUtils;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.TaskAttemptID;
import org.apache.hama.bsp.TaskLog;

/**
 * This class is responsible for launching and communicating with the child
 * process.
 * 
 * Adapted from Hadoop Pipes.
 * 
 */
public class Application<K1 extends Writable, V1 extends Writable, K2 extends Writable, V2 extends Writable, M extends Writable> {

  private static final Log LOG = LogFactory.getLog(Application.class.getName());
  private ServerSocket serverSocket;
  private Process process;
  private Socket clientSocket;

  private DownwardProtocol<K1, V1> downlink;

  static final boolean WINDOWS = System.getProperty("os.name").startsWith(
      "Windows");

  /**
   * Start the child process to handle the task for us.
   * 
   * @param peer the current peer including the task's configuration
   * @throws InterruptedException
   * @throws IOException
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  Application(BSPPeer<K1, V1, K2, V2, BytesWritable> peer) throws IOException,
      InterruptedException {

    Map<String, String> env = new HashMap<String, String>();
    boolean streamingEnabled = peer.getConfiguration().getBoolean(
        "hama.streaming.enabled", false);

    if (!streamingEnabled) {
      serverSocket = new ServerSocket(0);
      env.put("hama.pipes.command.port",
          Integer.toString(serverSocket.getLocalPort()));
    }
    // add TMPDIR environment variable with the value of java.io.tmpdir
    env.put("TMPDIR", System.getProperty("java.io.tmpdir"));

    /* Set Logging Environment from Configuration */
    env.put("hama.pipes.logging",
        peer.getConfiguration().getBoolean("hama.pipes.logging", false) ? "1"
            : "0");
    LOG.debug("DEBUG hama.pipes.logging: "
        + peer.getConfiguration().getBoolean("hama.pipes.logging", false));

    List<String> cmd = new ArrayList<String>();
    String interpretor = peer.getConfiguration().get(
        "hama.pipes.executable.interpretor");
    if (interpretor != null) {
      cmd.add(interpretor);
    }

    String executable = null;
    try {
      LOG.debug("DEBUG LocalCacheFilesCount: "
          + DistributedCache.getLocalCacheFiles(peer.getConfiguration()).length);
      for (Path u : DistributedCache
          .getLocalCacheFiles(peer.getConfiguration()))
        LOG.debug("DEBUG LocalCacheFiles: " + u);

      executable = DistributedCache.getLocalCacheFiles(peer.getConfiguration())[0]
          .toString();

      LOG.info("executable: " + executable);

    } catch (Exception e) {
      LOG.error("Executable: " + executable + " fs.default.name: "
          + peer.getConfiguration().get("fs.default.name"));

      throw new IOException("Executable is missing!");
    }

    if (!new File(executable).canExecute()) {
      // LinuxTaskController sets +x permissions on all distcache files already.
      // In case of DefaultTaskController, set permissions here.
      FileUtil.chmod(executable, "u+x");
    }
    cmd.add(executable);

    String additionalArgs = peer.getConfiguration().get(
        "hama.pipes.executable.args");
    // if true, we are resolving filenames with the linked paths in
    // DistributedCache
    boolean resolveArguments = peer.getConfiguration().getBoolean(
        "hama.pipes.resolve.executable.args", false);
    if (additionalArgs != null && !additionalArgs.isEmpty()) {
      String[] split = additionalArgs.split(" ");
      for (String s : split) {
        if (resolveArguments) {
          for (Path u : DistributedCache.getLocalCacheFiles(peer
              .getConfiguration())) {
            if (u.getName().equals(s)) {
              LOG.info("Resolved argument \"" + s
                  + "\" with fully qualified path \"" + u.toString() + "\"!");
              cmd.add(u.toString());
              break;
            }
          }
        } else {
          cmd.add(s);
        }
      }
    }

    // wrap the command in a stdout/stderr capture
    TaskAttemptID taskid = peer.getTaskId();
    File stdout = TaskLog.getTaskLogFile(taskid, TaskLog.LogName.STDOUT);
    File stderr = TaskLog.getTaskLogFile(taskid, TaskLog.LogName.STDERR);
    // Get the desired maximum length of task's logs.
    long logLength = TaskLog.getTaskLogLength(peer.getConfiguration());
    if (!streamingEnabled) {
      cmd = TaskLog.captureOutAndError(null, cmd, stdout, stderr, logLength);
    } else {
      // use tee in streaming to get the output to file
      cmd = TaskLog.captureOutAndErrorTee(null, cmd, stdout, stderr, logLength);
    }

    if (!stdout.getParentFile().exists()) {
      stdout.getParentFile().mkdirs();
      LOG.info("STDOUT: " + stdout.getParentFile().getAbsolutePath()
          + " created!");
    }
    LOG.info("STDOUT: " + stdout.getAbsolutePath());

    if (!stderr.getParentFile().exists()) {
      stderr.getParentFile().mkdirs();
      LOG.info("STDERR: " + stderr.getParentFile().getAbsolutePath()
          + " created!");
    }
    LOG.info("STDERR: " + stderr.getAbsolutePath());

    LOG.info("DEBUG: cmd: " + cmd);

    process = runClient(cmd, env); // fork c++ binary

    try {
      if (streamingEnabled) {
        downlink = new StreamingProtocol(peer, process.getOutputStream(),
            process.getInputStream());
      } else {
        LOG.info("DEBUG: waiting for Client at "
            + serverSocket.getLocalSocketAddress());
        serverSocket.setSoTimeout(2000);
        clientSocket = serverSocket.accept();
        downlink = new BinaryProtocol<K1, V1, K2, V2>(peer,
            clientSocket.getOutputStream(), clientSocket.getInputStream());
      }
      downlink.start();

    } catch (SocketException e) {
      throw new SocketException(
          "Timout: Client pipes application was not connecting!");
    }
  }

  /**
   * Get the downward protocol object that can send commands down to the
   * application.
   * 
   * @return the downlink proxy
   */
  DownwardProtocol<K1, V1> getDownlink() {
    return downlink;
  }

  /**
   * Wait for the application to finish
   * 
   * @return did the application finish correctly?
   * @throws IOException
   * @throws Throwable
   */
  boolean waitForFinish() throws InterruptedException, IOException {
    downlink.flush();
    return downlink.waitForFinish();
  }

  /**
   * Abort the application and wait for it to finish.
   * 
   * @param t the exception that signalled the problem
   * @throws IOException A wrapper around the exception that was passed in
   */
  void abort(Throwable t) throws IOException {
    LOG.info("Aborting because of " + StringUtils.stringifyException(t));
    try {
      downlink.abort();
      downlink.flush();
    } catch (IOException e) {
      // IGNORE cleanup problems
    }
    try {
      downlink.waitForFinish();
    } catch (Throwable ignored) {
      process.destroy();
    }
    IOException wrapper = new IOException("pipe child exception");
    wrapper.initCause(t);
    throw wrapper;
  }

  /**
   * Clean up the child process and socket if exist.
   */
  void cleanup() throws IOException {
    if (serverSocket != null) {
      serverSocket.close();
    }
    try {
      if (downlink != null) {
        downlink.close();
      }
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Run a given command in a subprocess, including threads to copy its stdout
   * and stderr to our stdout and stderr.
   * 
   * @param command the command and its arguments
   * @param env the environment to run the process in
   * @return a handle on the process
   * @throws IOException
   */
  static Process runClient(List<String> command, Map<String, String> env)
      throws IOException {
    ProcessBuilder builder = new ProcessBuilder(command);
    if (env != null) {
      builder.environment().putAll(env);
    }
    Process result = builder.start();
    return result;
  }

}
