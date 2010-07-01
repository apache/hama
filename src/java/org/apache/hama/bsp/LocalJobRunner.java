package org.apache.hama.bsp;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hama.Constants;
import org.apache.hama.ipc.InterTrackerProtocol;
import org.apache.hama.ipc.JobSubmissionProtocol;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

public class LocalJobRunner implements JobSubmissionProtocol {
  private static final Log LOG = LogFactory.getLog(BSPJobClient.class);
  private FileSystem fs;
  private Configuration conf;
  private int nextJobId = 1;
  private HashMap<String, Job> jobs = new HashMap<String, Job>();

  public LocalJobRunner(Configuration conf) throws IOException {
    this.fs = FileSystem.get(conf);
    this.conf = conf;
  }

  @Override
  public JobStatus[] getAllJobs() throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ClusterStatus getClusterStatus(boolean detailed) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getFilesystemName() throws IOException {
    return fs.getUri().toString();
  }

  @Override
  public JobProfile getJobProfile(BSPJobID jobid) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public JobStatus getJobStatus(BSPJobID jobid) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public BSPJobID getNewJobId() throws IOException {
    return new BSPJobID("local", nextJobId++);
  }

  @Override
  public String getSystemDir() {
    Path sysDir = new Path(conf.get("bsp.system.dir", "/tmp/hadoop/bsp/system"));
    return fs.makeQualified(sysDir).toString();
  }

  @Override
  public void killJob(BSPJobID jobid) throws IOException {
    jobs.get(jobid.toString()).done();
  }

  @Override
  public boolean killTask(TaskAttemptID taskId, boolean shouldFail)
      throws IOException {
    throw new UnsupportedOperationException("Killing tasks in "
        + "LocalJobRunner is not supported");
  }

  @Override
  public JobStatus submitJob(BSPJobID jobID, String jobFile) throws IOException {
    return new Job(jobID, jobFile, this.conf).status;
  }

  @Override
  public long getProtocolVersion(String protocol, long clientVersion)
      throws IOException {
    if (protocol.equals(InterTrackerProtocol.class.getName())) {
      return InterTrackerProtocol.versionID;
    } else if (protocol.equals(JobSubmissionProtocol.class.getName())) {
      return JobSubmissionProtocol.versionID;
    } else {
      throw new IOException("Unknown protocol to job tracker: " + protocol);
    }
  }

  /**
   * Local Job
   */
  private class Job extends Thread implements Watcher {
    private JobStatus status = new JobStatus();
    private Configuration conf;
    private int NUM_PEER;
    private BSPJob job;
    private String jobFile;
    private boolean threadDone = false;
    private HashMap<String, Task> tasks = new HashMap<String, Task>();

    public Job(BSPJobID jobID, String jobFile, Configuration conf)
        throws IOException {
      this.conf = conf;
      this.jobFile = jobFile;
      this.NUM_PEER = conf.getInt("bsp.peers.num", 0);
      LOG.info("LocalJobRunner: " + jobID + ", " + jobFile);
      this.job = new BSPJob(jobID, jobFile);
      LOG.info("Number of BSP tasks: " + NUM_PEER);
      jobs.put(jobID.toString(), this);

      ZooKeeper zk = new ZooKeeper(Constants.DEFAULT_ZOOKEEPER_SERVER_ADDR,
          3000, this);
      Stat s = null;
      if (zk != null) {
        try {
          s = zk.exists(Constants.DEFAULT_ZOOKEEPER_ROOT, false);
        } catch (Exception e) {
          LOG.error(s);
        }

        if (s == null) {
          try {
            zk.create(Constants.DEFAULT_ZOOKEEPER_ROOT, new byte[0],
                Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
          } catch (KeeperException e) {
            LOG.error(e);
          } catch (InterruptedException e) {
            LOG.error(e);
          }
        }
      }
      this.start();
    }

    public void run() {
      while (!threadDone) {
        TaskID tID;
        for (int i = 0; i < NUM_PEER; i++) {
          this.conf.set(Constants.PEER_PORT, String.valueOf(30000 + i));
          this.conf.setInt(Constants.PEER_ID, i);
          tID = new TaskID(job.getJobID(), false, i);

          Task bspRunner = new BSPTask(job.getJobID().getJtIdentifier(), jobFile, tID.toString(), i, this.conf);
          LOG.info("Adding task '" + tID.toString() + "' for '" + bspRunner.getName() + "'");
          tasks.put(tID.toString(), bspRunner);
        }

        // Launching tasks
        for (Map.Entry<String, Task> e : tasks.entrySet()) {
          e.getValue().runner.start();
        }

        // Barrier
        for (Map.Entry<String, Task> e : tasks.entrySet()) {
          try {
            e.getValue().join();
          } catch (InterruptedException e1) {
            e1.printStackTrace();
          }
        }
        done();
      }
    }

    public void done() {
      threadDone = true;
    }

    @Override
    public void process(WatchedEvent event) {
      // TODO Auto-generated method stub
    }
  }
}
