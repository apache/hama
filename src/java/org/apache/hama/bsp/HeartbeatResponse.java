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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 * A HeartbeatReponse class.
 */
public class HeartbeatResponse implements Writable, Configurable {
  private Configuration conf;
  short responseId;
  private GroomServerAction [] actions; 
  private Map<String, String> groomServers;

  public HeartbeatResponse() {}
  
  public HeartbeatResponse(short responseId, GroomServerAction [] actions,
      Map<String, String> groomServers) {
    this.responseId = responseId;
    this.actions = actions;
    this.groomServers = groomServers;
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

  public void setGroomServers(Map<String, String> groomServers) {
    this.groomServers = groomServers;
  }

  public Map<String, String> getGroomServers() {
    return groomServers;
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

    String[] groomServerNames = WritableUtils.readCompressedStringArray(in);
    String[] groomServerAddresses = WritableUtils.readCompressedStringArray(in);
    groomServers = new HashMap<String, String>(groomServerNames.length);

    for (int i = 0; i < groomServerNames.length; i++) {
      groomServers.put(groomServerNames[i], groomServerAddresses[i]);
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
        WritableUtils.writeEnum(out, action.getActionType());
        action.write(out);
      }
    }
    String[] groomServerNames = groomServers.keySet().toArray(new String[0]);
    WritableUtils.writeCompressedStringArray(out, groomServerNames);

    List<String> groomServerAddresses = new ArrayList<String>(groomServerNames.length);
    for (String groomName : groomServerNames) {
      groomServerAddresses.add(groomServers.get(groomName));
    }
    WritableUtils.writeCompressedStringArray(out, groomServerAddresses.toArray(new String[0]));
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
