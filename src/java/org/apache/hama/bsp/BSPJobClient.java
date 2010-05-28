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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;

import javax.security.auth.login.LoginException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UnixUserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hama.bsp.BSPMaster;
import org.apache.hama.ipc.JobSubmissionProtocol;

public class BSPJobClient extends Configured {
  private static final Log LOG = LogFactory.getLog(BSPJobClient.class);
  public static enum TaskStatusFilter { NONE, KILLED, FAILED, SUCCEEDED, ALL }
  private static final long MAX_JOBPROFILE_AGE = 1000 * 2;

  static {
    Configuration.addDefaultResource("hama-default.xml");
    Configuration.addDefaultResource("hama-site.xml");
  }

  class NetworkedJob implements RunningJob {
    JobProfile profile;
    JobStatus status;
    long statustime;
    
    public NetworkedJob(JobStatus job) throws IOException {
      this.status = job;
      this.profile = jobSubmitClient.getJobProfile(job.getJobID());
      this.statustime = System.currentTimeMillis();
    }
    
    /**
     * Some methods rely on having a recent job profile object.  Refresh
     * it, if necessary
     */
    synchronized void ensureFreshStatus() throws IOException {
      if (System.currentTimeMillis() - statustime > MAX_JOBPROFILE_AGE) {
        updateStatus();
      }
    }
    
    /** Some methods need to update status immediately. So, refresh
     * immediately
     * @throws IOException
     */
    synchronized void updateStatus() throws IOException {
      this.status = jobSubmitClient.getJobStatus(profile.getJobID());
      this.statustime = System.currentTimeMillis();
    }

    /* (non-Javadoc)
     * @see org.apache.hama.bsp.RunningJob#getID()
     */
    @Override
    public BSPJobID getID() {      
      return profile.getJobID();
    }
    
    /* (non-Javadoc)
     * @see org.apache.hama.bsp.RunningJob#getJobName()
     */
    @Override
    public String getJobName() {
      return profile.getJobName();
    }

    /* (non-Javadoc)
     * @see org.apache.hama.bsp.RunningJob#getJobFile()
     */
    @Override
    public String getJobFile() {
      return profile.getJobFile();
    }
    
    @Override
    public float progress() throws IOException {
      ensureFreshStatus();
      return status.progress();
    }
    
    /**
     * Returns immediately whether the whole job is done yet or not.
     */
    public synchronized boolean isComplete() throws IOException {
      updateStatus();
      return (status.getRunState() == JobStatus.SUCCEEDED ||
              status.getRunState() == JobStatus.FAILED ||
              status.getRunState() == JobStatus.KILLED);
    }
    
    /**
     * True iff job completed successfully.
     */
    public synchronized boolean isSuccessful() throws IOException {
      updateStatus();
      return status.getRunState() == JobStatus.SUCCEEDED;
    }
    
    /**
     * Blocks until the job is finished
     */
    public void waitForCompletion() throws IOException {
      while (!isComplete()) {
        try {
          Thread.sleep(5000);
        } catch (InterruptedException ie) {
        }
      }
    }

    /**
     * Tells the service to get the state of the current job.
     */
    public synchronized int getJobState() throws IOException {
      updateStatus();
      return status.getRunState();
    }
    
    /**
     * Tells the service to terminate the current job.
     */
    public synchronized void killJob() throws IOException {
      jobSubmitClient.killJob(getID());
    }

    @Override
    public void killTask(TaskAttemptID taskId, boolean shouldFail)
        throws IOException {
      jobSubmitClient.killTask(taskId, shouldFail);      
    }    
  }
  
  private JobSubmissionProtocol jobSubmitClient = null;
  private Path sysDir = null;
  private FileSystem fs = null;
  
  // job files are world-wide readable and owner writable
  final private static FsPermission JOB_FILE_PERMISSION = 
    FsPermission.createImmutable((short) 0644); // rw-r--r--

  // job submission directory is world readable/writable/executable
  final static FsPermission JOB_DIR_PERMISSION =
    FsPermission.createImmutable((short) 0777); // rwx-rwx-rwx

  public BSPJobClient() {
  }

  public BSPJobClient(Configuration conf) throws IOException {
    setConf(conf);
    init(conf);
  }

  public void init(Configuration conf) throws IOException {
    // it will be used to determine if the bspmaster is running on local or not. 
    //String tracker = conf.get("bsp.master.address", "local"); 
    this.jobSubmitClient = createRPCProxy(BSPMaster.getAddress(conf), conf);
  }

  private JobSubmissionProtocol createRPCProxy(InetSocketAddress addr,
      Configuration conf) throws IOException {
    return (JobSubmissionProtocol) RPC.getProxy(JobSubmissionProtocol.class,
        JobSubmissionProtocol.versionID, addr, conf, NetUtils.getSocketFactory(
            conf, JobSubmissionProtocol.class));
  }
  
  /**
   * Close the <code>JobClient</code>.
   */
  public synchronized void close() throws IOException {    
      RPC.stopProxy(jobSubmitClient);
  }
  
  /**
   * Get a filesystem handle.  We need this to prepare jobs
   * for submission to the BSP system.
   * 
   * @return the filesystem handle.
   */
  public synchronized FileSystem getFs() throws IOException {
    if (this.fs == null) {
      Path sysDir = getSystemDir();
      this.fs = sysDir.getFileSystem(getConf());
    }
    return fs;
  }
  
  /* see if two file systems are the same or not
  *
  */ /*
 private boolean compareFs(FileSystem srcFs, FileSystem destFs) {
   URI srcUri = srcFs.getUri();
   URI dstUri = destFs.getUri();
   if (srcUri.getScheme() == null) {
     return false;
   }
   if (!srcUri.getScheme().equals(dstUri.getScheme())) {
     return false;
   }
   String srcHost = srcUri.getHost();    
   String dstHost = dstUri.getHost();
   if ((srcHost != null) && (dstHost != null)) {
     try {
       srcHost = InetAddress.getByName(srcHost).getCanonicalHostName();
       dstHost = InetAddress.getByName(dstHost).getCanonicalHostName();
     } catch(UnknownHostException ue) {
       return false;
     }
     if (!srcHost.equals(dstHost)) {
       return false;
     }
   }
   else if (srcHost == null && dstHost != null) {
     return false;
   }
   else if (srcHost != null && dstHost == null) {
     return false;
   }
   //check for ports
   if (srcUri.getPort() != dstUri.getPort()) {
     return false;
   }
   return true;
 } */
  
  //copies a file to the bspmaster filesystem and returns the path where it
  // was copied to
 /*
  private Path copyRemoteFiles(FileSystem jtFs, Path parentDir, Path originalPath, 
                               BSPJobContext job, short replication) throws IOException {
    //check if we do not need to copy the files
    // is jt using the same file system.
    // just checking for uri strings... doing no dns lookups 
    // to see if the filesystems are the same. This is not optimal.
    // but avoids name resolution.
    
    FileSystem remoteFs = null;
    remoteFs = originalPath.getFileSystem(job.getConf());
    if (compareFs(remoteFs, jtFs)) {
      return originalPath;
    }
    // this might have name collisions. copy will throw an exception
    //parse the original path to create new path
    Path newPath = new Path(parentDir, originalPath.getName());
    FileUtil.copy(remoteFs, originalPath, jtFs, newPath, false, job.getConf());
    jtFs.setReplication(newPath, replication);
    return newPath;
  }*/
  
  private UnixUserGroupInformation getUGI(Configuration conf) throws IOException {
    UnixUserGroupInformation ugi = null;
    try {
      ugi = UnixUserGroupInformation.login(conf, true);
    } catch (LoginException e) {
      throw (IOException)(new IOException(
          "Failed to get the current user's information.").initCause(e));
    }
    return ugi;
  }
  
  /**
   * Submit a job to the BSP system.
   * This returns a handle to the {@link RunningJob} which can be used to track
   * the running-job.
   * 
   * @param job the job configuration.
   * @return a handle to the {@link RunningJob} which can be used to track the
   *         running-job.
   * @throws FileNotFoundException
   * @throws IOException
   */
  public RunningJob submitJob(BSPJob job) throws FileNotFoundException,
                                                  IOException {    
      return submitJobInternal(job);    
  }
  
  public 
  RunningJob submitJobInternal(BSPJob job) throws IOException {
    BSPJobID jobId = jobSubmitClient.getNewJobId();    
    Path submitJobDir = new Path(getSystemDir(), jobId.toString());
    Path submitJarFile = new Path(submitJobDir, "job.jar");    
    Path submitJobFile = new Path(submitJobDir, "job.xml");
    
    /*
     * set this user's id in job configuration, so later job files can be
     * accessed using this user's id
     */
    UnixUserGroupInformation ugi = getUGI(job.getConf());
    
    // Create a number of filenames in the BSPMaster's fs namespace
    FileSystem fs = getFs();
    LOG.debug("default FileSystem: " + fs.getUri());
    fs.delete(submitJobDir, true);
    submitJobDir = fs.makeQualified(submitJobDir);
    submitJobDir = new Path(submitJobDir.toUri().getPath());
    FsPermission bspSysPerms = new FsPermission(JOB_DIR_PERMISSION);
    FileSystem.mkdirs(fs, submitJobDir, bspSysPerms);
    short replication = (short)job.getInt("bsp.submit.replication", 10);
    
    String originalJarPath = job.getJar();

    if (originalJarPath != null) { // copy jar to BSPMaster's fs
      // use jar name if job is not named. 
      if ("".equals(job.getJobName())){
        job.setJobName(new Path(originalJarPath).getName());
      }
      job.setJar(submitJarFile.toString());
      fs.copyFromLocalFile(new Path(originalJarPath), submitJarFile);
      fs.setReplication(submitJarFile, replication);
      fs.setPermission(submitJarFile, new FsPermission(JOB_FILE_PERMISSION));
    } else {
      LOG.warn("No job jar file set.  User classes may not be found. "+
               "See BSPJobConf#setJar(String) or check Your jar file.");
    }
    
    // Set the user's name and working directory
    job.setUser(ugi.getUserName());
    if (ugi.getGroupNames().length > 0) {
      job.set("group.name", ugi.getGroupNames()[0]);
    }
    if (job.getWorkingDirectory() == null) {
      job.setWorkingDirectory(fs.getWorkingDirectory());          
    }
    
    // Write job file to BSPMaster's fs        
    FSDataOutputStream out = 
      FileSystem.create(fs, submitJobFile,
                        new FsPermission(JOB_FILE_PERMISSION));
    
    try {
      job.writeXml(out);
    } finally {
      out.close();
    }
    
    //
    // Now, actually submit the job (using the submit name)
    //
    JobStatus status = jobSubmitClient.submitJob(jobId);
    if (status != null) {
      return new NetworkedJob(status);
    } else {
      throw new IOException("Could not launch job");
    }
  }

  /**
   * Monitor a job and print status in real-time as progress is made and tasks 
   * fail.
   * 
   * @param job
   * @param info
   * @return true, if job is successful
   * @throws IOException
   * @throws InterruptedException
   */
  public boolean monitorAndPrintJob (BSPJob job, RunningJob info) 
    throws IOException, InterruptedException {
    
    String lastReport = null;
    BSPJobID jobId = job.getJobID();
    LOG.info("Running job: " + jobId);
    
    while (!job.isComplete()) {
      Thread.sleep(1000);
      String report = " bsp " + StringUtils.formatPercent(job.progress(), 0);
      
      if (!report.equals(lastReport)) {
        LOG.info(report);
        lastReport = report;
      }      
    }
    
    LOG.info("Job complete: " + jobId);
    return job.isSuccessful();
  }
  
  /**
   * Grab the bspmaster system directory path where job-specific files are to be placed.
   * 
   * @return the system directory where job-specific files are to be placed.
   */
  public Path getSystemDir() {
    if (sysDir == null) {
      sysDir = new Path(jobSubmitClient.getSystemDir());
    }
    return sysDir;
  }

  public RunningJob runJob(BSPJob job) throws FileNotFoundException,
  IOException {
    return submitJobInternal(job);    
  }
  
  /**
   * Get status information about the BSP cluster
   * 
   * @throws IOException
   */
  public ClusterStatus getClusterStatus() throws IOException {
    // TODO: 
    
    return null;
  }
}
