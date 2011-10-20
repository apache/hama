package org.apache.hama.bsp.sync.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.bsp.BSPJobID;
import org.apache.hama.bsp.TaskAttemptID;
import org.apache.hama.bsp.sync.SyncClient;

/**
 * Client side implementation of an example RPC sync client. Basically it just
 * proxies all the calls to the RPC server and returns the result.
 */
public class RPCSyncClientImpl implements SyncClient {

  private RPCSyncServer syncService;

  @Override
  public void init(Configuration conf, BSPJobID jobId, TaskAttemptID taskId)
      throws Exception {
    syncService = RPCSyncServerImpl.getService(conf);
  }

  @Override
  public void enterBarrier(BSPJobID jobId, TaskAttemptID taskId, long superstep)
      throws Exception {
    syncService.enterBarrier(taskId);
  }

  @Override
  public void leaveBarrier(BSPJobID jobId, TaskAttemptID taskId, long superstep)
      throws Exception {
    syncService.leaveBarrier(taskId);
  }

  @Override
  public void register(BSPJobID jobId, TaskAttemptID taskId,
      String hostAddress, long port) {
    syncService.register(taskId, new Text(hostAddress), new LongWritable(port));
  }

  @Override
  public String[] getAllPeerNames(TaskAttemptID taskId) {
    // our sync service ensures the order of the peers
    return syncService.getAllPeerNames().get();
  }

  @Override
  public void deregisterFromBarrier(BSPJobID jobId, TaskAttemptID taskId,
      String hostAddress, long port) {
    syncService.deregisterFromBarrier(taskId, new Text(hostAddress),
        new LongWritable(port));
  }

  @Override
  public void stopServer() {
    syncService.stopServer();
  }

  @Override
  public void close() throws Exception {
    
  }

}
