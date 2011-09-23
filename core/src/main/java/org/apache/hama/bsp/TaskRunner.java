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
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.RunJar;
import org.apache.hama.checkpoint.CheckpointRunner;

/**
 * Base class that runs a task in a separate process.
 */
public class TaskRunner extends Thread {

	public static final Log LOG = LogFactory.getLog(TaskRunner.class);
	private static final String SYSTEM_PATH_SEPARATOR = System
			.getProperty("path.separator");

	private enum LogType {
		STDOUT, ERROR
	}

	boolean bspKilled = false;
	private Process bspProcess;

	private final Task task;
	private final BSPJob conf;
	private final GroomServer groomServer;

	private File logDir;

	class BspChildRunner implements Callable<Object> {
		private final List<String> commands;
		private final File workDir;
		private final ScheduledExecutorService sched;
		private final AtomicReference<ScheduledFuture<Object>> future;

		BspChildRunner(List<String> commands, File workDir) {
			this.commands = commands;
			this.workDir = workDir;
			this.sched = Executors.newScheduledThreadPool(1);
			this.future = new AtomicReference<ScheduledFuture<Object>>();
		}

		void start() {
			this.future.set(this.sched.schedule(this, 0, SECONDS));
			LOG.info("Start building BSPPeer process.");
		}

		void stop() {
			killBsp();
			this.sched.schedule(this, 0, SECONDS);
			LOG.info("Stop BSPPeer process.");
		}

		void join() throws InterruptedException, ExecutionException {
			this.future.get().get();
		}

		public Object call() throws Exception {
			ProcessBuilder builder = new ProcessBuilder(commands);
			builder.directory(workDir);
			try {
				bspProcess = builder.start();
				new Thread() {
					public void run() {
						logStream(bspProcess.getErrorStream(), LogType.ERROR);
					}
				}.start();

				new Thread() {
					public void run() {
						logStream(bspProcess.getInputStream(), LogType.STDOUT);
					}
				}.start();

				int exit_code = bspProcess.waitFor();
				if (!bspKilled && exit_code != 0) {
					throw new IOException(
							"BSP task process exit with nonzero status of "
									+ exit_code + ".");
				}
			} catch (InterruptedException e) {
				LOG.warn(
						"Thread is interrupted when execeuting Checkpointer process.",
						e);
			} catch (IOException ioe) {
				LOG.error("Error when executing BSPPeer process.", ioe);
			} finally {
				killBsp();
			}
			return null;
		}
	}

	public TaskRunner(BSPTask bspTask, GroomServer groom, BSPJob conf) {
		this.task = bspTask;
		this.conf = conf;
		this.groomServer = groom;
	}

	public Task getTask() {
		return task;
	}

	/**
	 * Called to assemble this task's input. This method is run in the parent
	 * process before the child is spawned. It should not execute user code,
	 * only system code.
	 */
	public boolean prepare() throws IOException {
		return true;
	}

	private File createWorkDirectory() {
		File workDir = new File(new File(task.getJobFile()).getParent(), "work");
		boolean isCreated = workDir.mkdirs();
		if (isCreated) {
			LOG.debug("TaskRunner.workDir : " + workDir);
		}
		return workDir;
	}

	private String assembleClasspath(BSPJob jobConf, File workDir) {
		StringBuffer classPath = new StringBuffer();
		// start with same classpath as parent process
		classPath.append(System.getProperty("java.class.path"));
		classPath.append(SYSTEM_PATH_SEPARATOR);

		String jar = jobConf.getJar();
		if (jar != null) { // if jar exists, it into workDir
			try {
				RunJar.unJar(new File(jar), workDir);
			} catch (IOException ioe) {
				LOG.error(
						"Unable to uncompressing file to " + workDir.toString(),
						ioe);
			}
			File[] libs = new File(workDir, "lib").listFiles();
			if (libs != null) {
				for (int i = 0; i < libs.length; i++) {
					// add libs from jar to classpath
					classPath.append(SYSTEM_PATH_SEPARATOR);
					classPath.append(libs[i]);
				}
			}
			classPath.append(SYSTEM_PATH_SEPARATOR);
			classPath.append(new File(workDir, "classes"));
			classPath.append(SYSTEM_PATH_SEPARATOR);
			classPath.append(workDir);
		}
		return classPath.toString();
	}

	private List<String> buildJvmArgs(BSPJob jobConf, String classPath,
			Class<?> child) {
		// Build exec child jmv args.
		List<String> vargs = new ArrayList<String>();
		File jvm = // use same jvm as parent
		new File(new File(System.getProperty("java.home"), "bin"), "java");
		vargs.add(jvm.toString());

		// bsp.child.java.opts
		String javaOpts = jobConf.getConf().get("bsp.child.java.opts",
				"-Xmx200m");
		javaOpts = javaOpts.replace("@taskid@", task.getTaskID().toString());

		String[] javaOptsSplit = javaOpts.split(" ");
		for (int i = 0; i < javaOptsSplit.length; i++) {
			vargs.add(javaOptsSplit[i]);
		}

		// Add classpath.
		vargs.add("-classpath");
		vargs.add(classPath.toString());
		// Add main class and its arguments
		LOG.debug("Executing child Process " + child.getName());
		vargs.add(child.getName()); // main of bsp or checkpointer Child

		if (GroomServer.BSPPeerChild.class.equals(child)) {
			InetSocketAddress addr = groomServer.getTaskTrackerReportAddress();
			vargs.add(addr.getHostName());
			vargs.add(Integer.toString(addr.getPort()));
			vargs.add(task.getTaskID().toString());
			vargs.add(groomServer.groomHostName);
		}

		if (jobConf.getConf().getBoolean("bsp.checkpoint.enabled", false)) {
			String ckptPort = jobConf.getConf().get("bsp.checkpoint.port",
					CheckpointRunner.DEFAULT_PORT);
			LOG.debug("Checkpointer's port:" + ckptPort);
			vargs.add(ckptPort);
		}

		return vargs;
	}

	/**
	 * Build working environment and launch BSPPeer and Checkpointer processes.
	 * And transmit data from BSPPeer's inputstream to Checkpointer's
	 * OutputStream.
	 */
	public void run() {
		File workDir = createWorkDirectory();
		logDir = createLogDirectory();
		String classPath = assembleClasspath(conf, workDir);
		LOG.debug("Spawned child's classpath " + classPath);
		List<String> bspArgs = buildJvmArgs(conf, classPath,
				GroomServer.BSPPeerChild.class);

		BspChildRunner bspPeer = new BspChildRunner(bspArgs, workDir);
		bspPeer.start();
		try {
			bspPeer.join();
		} catch (InterruptedException ie) {
			LOG.error("BSPPeer child process is interrupted.", ie);
		} catch (ExecutionException ee) {
			LOG.error("Failure occurs when retrieving tasks result.", ee);
		}
		LOG.info("Finishes executing BSPPeer child process.");
	}

	/**
	 * Creates the tasks log directory if needed.
	 * 
	 * @return the top directory of the tasks logging area.
	 */
	private File createLogDirectory() {
		// our log dir looks following: log/tasklogs/job_id/
		File f = new File(System.getProperty("hama.log.dir") + File.separator
				+ "tasklogs" + File.separator + task.jobId.id);
		// TODO if we have attemps: + File.separator+ task.getTaskID());

		if (!f.exists()) {
			f.mkdirs();
		}

		return f;
	}

	/**
	 * Kill bsppeer child process.
	 */
	public void killBsp() {
		if (bspProcess != null) {
			bspProcess.destroy();
		}
		bspKilled = true;
	}

	/**
	 * Log process's stream.
	 * 
	 * @param input
	 *            stream to be logged.
	 * @param stdout
	 *            type of the log
	 */
	private void logStream(InputStream input, LogType type) {
		// STDOUT file can be found under LOG_DIR/task_attempt_id.log
		// ERROR file can be found under LOG_DIR/task_attempt_id.err
		File taskLogFile = new File(logDir, task.getTaskAttemptId()
				+ getFileEndingForType(type));
		BufferedWriter writer = null;
		try {
			writer = new BufferedWriter(new FileWriter(taskLogFile));
			BufferedReader in = new BufferedReader(new InputStreamReader(input));
			String line;
			while ((line = in.readLine()) != null) {
				writer.write(line);
				writer.newLine();
			}
		} catch (IOException e) {
			LOG.warn(task.getTaskID() + " Error reading child output", e);
		} finally {
			try {
				input.close();
			} catch (IOException e) {
				LOG.warn(task.getTaskID() + " Error closing child output", e);
			}
			try {
				writer.close();
			} catch (IOException e) {
				LOG.warn(task.getTaskID() + " Error closing log file", e);
			}
		}
	}

	/**
	 * Returns the ending of the logfile for each LogType. e.G. ".log".
	 * 
	 * @param type
	 * @return an ending, including a dot.
	 */
	private String getFileEndingForType(LogType type) {
		if (type != LogType.ERROR)
			return ".err";
		else
			return ".log";
	}
}
