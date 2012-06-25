/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hama.bsp.sync;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;

public abstract class ZKSyncEventListener extends SyncEventListener
 implements Watcher {
Log LOG = LogFactory.getLog(SyncEventListener.class);
  
  private ZKSyncClient client;
  private SyncEvent event;

  /**
   * 
   */
  @Override
  public void process(WatchedEvent event) {
    
    client.registerListener(event.getPath(),
        ZKSyncEventFactory.getValueChangeEvent()
        , this);    
    //if(LOG.isDebugEnabled()){
      LOG.debug(event.toString());
    //}

    if(event.getType().equals(EventType.NodeChildrenChanged)){
      LOG.debug("Node children changed - " + event.getPath());
      onChildKeySetChange();
    }
    else if (event.getType().equals(EventType.NodeDeleted)){
      LOG.debug("Node children deleted - " + event.getPath());
      onDelete();
    }
    else if (event.getType().equals(EventType.NodeDataChanged)){
      LOG.debug("Node children changed - " + event.getPath());
      
      onChange();
    }

  }
  
  public void setZKSyncClient(ZKSyncClient zkClient){
    client = zkClient;
  }
  
  public void setSyncEvent(SyncEvent event){
    this.event = event;
  }
  
  public SyncEvent getEvent(){
    return this.event;
  }

  /**
   * 
   */
  public abstract void onDelete();

  /**
   * 
   */
  public abstract void onChange();

  /**
   * 
   */
  public abstract void onChildKeySetChange();

  @Override
  public void handleEvent(int eventId) {
    
  }

}
