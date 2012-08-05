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

/**
 * Zookeeper Synchronization Event Factory. 
 * <ul>It provides three event definitions. 
 * <li>Value stored in a Zookeeper node is changed
 * <li>A new child node is added to a Zookeeper node.
 * <li>A Zookeeper node is deleted.
 * </ul>
 */
public class ZKSyncEventFactory {
  
  public static enum ZKEvent{
    VALUE_CHANGE_EVENT(0),
    CHILD_ADD_EVENT(1),
    DELETE_EVENT(2);
    
    private final int id;
    
    ZKEvent(int num){
      this.id = num;
    }
    
    public int getValue(){
      return this.id;
    }
    
    public static int getEventCount(){
      return ZKEvent.values().length;
    }
    
    public String getName(int num){
      if(num >=0 && num < ZKEvent.getEventCount()){
        return ZKEvent.values()[num].name();
      }
      else 
        throw new IllegalArgumentException((new StringBuilder(100)
              .append("The value ")
              .append(num).append(" is not a valid ZKEvent type. ")
              .append("Expected range is 0-")
              .append(getEventCount()-1)).toString());
    }
    
  };
  
  public static int getSupportedEventCount(){
    return ZKEvent.getEventCount();
  }

  private static class ValueChangeEvent implements SyncEvent {

    @Override
    public int getEventId() {
      return ZKEvent.VALUE_CHANGE_EVENT.getValue();
    }
    
  }
  
  private static class ChildAddEvent implements SyncEvent {

    @Override
    public int getEventId() {
      return ZKEvent.CHILD_ADD_EVENT.getValue();
    }
    
  }
  
  private static class DeleteEvent implements SyncEvent {

    @Override
    public int getEventId() {
      return ZKEvent.DELETE_EVENT.getValue();
    }
    
  }
  
  /**
   * Provides the Zookeeper node value change event definition.
   * @return the Zookeeper value changed event.
   */
  public static SyncEvent getValueChangeEvent(){
    return new ValueChangeEvent();
  }
  
  /**
   * Provides the Zookeeper deletion event definition.
   * @return the Zookeeper node is deleted event
   */
  public static SyncEvent getDeletionEvent(){
    return new DeleteEvent();
  }
  
  /**
   * Provides the Zookeeper child addition event definition. 
   * @return the Zookeeper child node is added event
   */
  public static SyncEvent getChildAddEvent(){
    return new ChildAddEvent();
  }
  

}
