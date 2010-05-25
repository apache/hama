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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class HeartbeatResponse implements Writable, Configurable {
  private Configuration conf;
  short responseId;
  private GroomServerAction [] actions; 

  public HeartbeatResponse() {}
  
  public HeartbeatResponse(short responseId, GroomServerAction [] actions) {
    this.responseId = responseId;
    this.actions = actions;
  }

  public void setResponseId(short responseId) {
    this.responseId = responseId;
  }

  public short getResponseId() {
    return responseId;
  }
  
  public void setActions(GroomServerAction [] actions) {
    this.actions = actions;
  }
  
  public GroomServerAction [] getActions() {
    return actions;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.responseId = in.readShort();
    int length = WritableUtils.readVInt(in);
    if (length > 0) {
      actions = new GroomServerAction[length];
      for(int i=0; i< length; ++i) {
        GroomServerAction.ActionType actionType = 
          WritableUtils.readEnum(in, GroomServerAction.ActionType.class);
        actions[i] = GroomServerAction.createAction(actionType);
        actions[i].readFields(in);
      }
    } else {
      actions = null;
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeShort(this.responseId);
    if(actions == null) {
      WritableUtils.writeVInt(out, 0);
    } else {
      WritableUtils.writeVInt(out, actions.length);
      for(GroomServerAction action: actions) {
        WritableUtils.writeEnum(out, action.getActionId());
        action.write(out);
      }
    }
  }

  @Override
  public Configuration getConf() {
    return this.conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }
}
