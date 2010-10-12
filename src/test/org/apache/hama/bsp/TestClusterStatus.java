package org.apache.hama.bsp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import junit.framework.TestCase;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hama.bsp.BSPMaster;
import org.apache.hama.bsp.ClusterStatus;

public class TestClusterStatus extends TestCase {
  Random rnd = new Random();
  
  protected void setUp() throws Exception {
    super.setUp();
  }

  public final void testWriteAndReadFields() throws IOException {
    DataOutputBuffer out = new DataOutputBuffer();
    DataInputBuffer in = new DataInputBuffer();
    
    ClusterStatus status1;
    List<String> grooms = new ArrayList<String>();
        
    for(int i=0;i< 10;i++) {
      grooms.add("groom_"+rnd.nextInt());
    }
    
    int tasks = rnd.nextInt(100);
    int maxTasks = rnd.nextInt(100);
    BSPMaster.State state = BSPMaster.State.RUNNING;
    
    status1 = new ClusterStatus(grooms, tasks, maxTasks, state);    
    status1.write(out);
    
    in.reset(out.getData(), out.getLength());
   
    ClusterStatus status2 = new ClusterStatus();
    status2.readFields(in);
    
    Set<String> grooms_s = new HashSet<String>(status1.getActiveGroomNames());
    Set<String> grooms_o = new HashSet<String>(status2.getActiveGroomNames());     
    
    assertEquals(status1.getGroomServers(), status2.getGroomServers());
    
    assertTrue(grooms_s.containsAll(grooms_o));
    assertTrue(grooms_o.containsAll(grooms_s));    
    
    assertEquals(status1.getTasks(),status2.getTasks());
    assertEquals(status1.getMaxTasks(), status2.getMaxTasks());
  }
}
