/**
 * Copyright 2007 The Apache Software Foundation
 *
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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hama.HamaCluster;
import org.apache.hama.HamaConfiguration;

public class TestDirective extends HamaCluster {
  private Log LOG = LogFactory.getLog(TestDirective.class);

  HamaConfiguration conf;

  public TestDirective() {
    this.conf = getConf();
  }

  public void setUp() throws Exception {
  }

  public void testRequest() throws Exception{
    BSPJobID jobId = new BSPJobID();
    TaskID taskId = new TaskID(jobId, true, 0);
    TaskAttemptID attemptId = new TaskAttemptID(taskId, 0);
    GroomServerAction[] actions = new GroomServerAction[]{ 
      new LaunchTaskAction(new BSPTask(jobId, "/path/to/jobFile", attemptId,0))};
    Map<String, String> groomServerPeers = new HashMap<String, String>();
    groomServerPeers.put("groomServer1", "192.168.0.22:8080");
    groomServerPeers.put("groomServer2", "192.168.0.23:8081");
    groomServerPeers.put("groomServer3", "192.168.0.24:8082");
    groomServerPeers.put("groomServer4", "192.168.0.25:8083");
    groomServerPeers.put("groomServer5", "192.168.0.26:8084");

    Directive w = new Directive(groomServerPeers, actions);

    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    assertEquals("Check if data is serialized to output stream.", 
                 bout.size(), 0);

    DataOutput out = new DataOutputStream(bout);
    w.write(out);
   
    assertTrue("Check if data is serialized to output stream.", 
               bout.size() > 0 );
  
    byte[] bytes = bout.toByteArray();

    ByteArrayInputStream bin = new ByteArrayInputStream(bytes);
    DataInput in = new DataInputStream(bin);
    
    Directive r = new Directive();
    r.readFields(in);

    assertEquals("Check the reqeust type.", Directive.Type.Request.value(),
                 r.getType().value()); 

    Map<String, String> peers = r.getGroomServerPeers();
    assertEquals("Check groom server peers content.", 
                 peers.get("groomServer1"), 
                 groomServerPeers.get("groomServer1"));
    assertEquals("Check groom server peers content.", 
                 peers.get("groomServer2"), 
                 groomServerPeers.get("groomServer2"));
    assertEquals("Check groom server peers content.", 
                 peers.get("groomServer3"), 
                 groomServerPeers.get("groomServer3"));
    assertEquals("Check groom server peers content.", 
                 peers.get("groomServer4"), 
                 groomServerPeers.get("groomServer4"));
    assertEquals("Check groom server peers content.", 
                 peers.get("groomServer5"), 
                 groomServerPeers.get("groomServer5"));

    GroomServerAction[] as = r.getActions();
    assertEquals("Check GroomServerAction size.", as.length, actions.length);

    assertTrue("Check GroomServerAction type.", 
               as[0] instanceof LaunchTaskAction);

    Task t = ((LaunchTaskAction)as[0]).getTask();

    assertEquals("Check action's bsp job id.", t.getJobID(), jobId);

    assertEquals("Check action's job file.", 
                 t.getJobFile(), "/path/to/jobFile");

    assertEquals("Check action's partition.", t.getPartition(), 0);

  }

  public void testResponse() throws Exception{

    BSPJobID jobId = new BSPJobID();
    TaskID taskId = new TaskID(jobId, true, 0);
    TaskAttemptID attemptId = new TaskAttemptID(taskId, 0);

    List<TaskStatus> tasks = new ArrayList<TaskStatus>();
    tasks.add(new TaskStatus(jobId, attemptId, 1f, 
      TaskStatus.State.SUCCEEDED, "", "groomServer1", 
      TaskStatus.Phase.CLEANUP));
    GroomServerStatus status = new GroomServerStatus("groomServer1", 
      "192.168.1.111:2123", tasks, 1, 4);
    Directive w = new Directive(status);
    assertEquals("Check directive type is correct.", 
                 w.getType().value(), Directive.Type.Response.value());

    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    DataOutput out = new DataOutputStream(bout);
    w.write(out);

    assertTrue("Check if output has data.", bout.size() > 0); 

    byte[] bytes = bout.toByteArray();

    ByteArrayInputStream bin = new ByteArrayInputStream(bytes);
    DataInput in = new DataInputStream(bin);

    Directive r = new Directive();
    r.readFields(in);

    assertEquals("Check directive type is correct.", 
                 r.getType().value(), w.getType().value());

    assertEquals("Check groom server status' groom name.",
                 r.getStatus().getGroomName(), "groomServer1");
    
    assertEquals("Check groom server status' peer name.",
                 r.getStatus().getPeerName(), "192.168.1.111:2123");
    TaskStatus t = r.getStatus().getTaskReports().get(0);

    assertEquals("Check tasks status' job id.", t.getJobId(), jobId);
    
    assertEquals("Check task status' run state.",
                 t.getRunState(), TaskStatus.State.SUCCEEDED);

    assertEquals("Check task status'  state.",
                 t.getPhase(), TaskStatus.Phase.CLEANUP);
    
  }

  public void tearDown() throws Exception{
  }
}
