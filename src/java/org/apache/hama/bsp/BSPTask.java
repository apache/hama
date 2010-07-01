package org.apache.hama.bsp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;

public class BSPTask extends Task {
  
  public BSPTask(String jobId, String jobFile, String taskid, int partition, Configuration conf) {
    this.jobId = jobId;
    this.jobFile = jobFile;
    this.taskId = taskid;
    this.partition = partition;
    this.runner = (BSPRunner) ReflectionUtils.newInstance(
        BSPRunner.class, conf);
  }

}
