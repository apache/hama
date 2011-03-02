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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 * A generic directive from the {@link org.apache.hama.bsp.BSPMaster} to the
 * {@link org.apache.hama.bsp.GroomServer} to take some 'action'.
 */
public class Directive implements Writable {

  public static final Log LOG = LogFactory.getLog(Directive.class);

  private long timestamp;
  private Directive.Type type;
  private Map<String, String> groomServerPeers;
  private GroomServerAction[] actions;
  private GroomServerStatus status;

  public static enum Type {
    Request(1), Response(2);
    int t;

    Type(int t) {
      this.t = t;
    }

    public int value() {
      return this.t;
    }
  };

  public Directive() {
    this.timestamp = System.currentTimeMillis();
  }

  public Directive(Map<String, String> groomServerPeers,
      GroomServerAction[] actions) {
    this();
    this.type = Directive.Type.Request;
    this.groomServerPeers = groomServerPeers;
    this.actions = actions;
  }

  public Directive(GroomServerStatus status) {
    this();
    this.type = Directive.Type.Response;
    this.status = status;
  }

  public long getTimestamp() {
    return this.timestamp;
  }

  public Directive.Type getType() {
    return this.type;
  }

  public Map<String, String> getGroomServerPeers() {
    return this.groomServerPeers;
  }

  public GroomServerAction[] getActions() {
    return this.actions;
  }

  public GroomServerStatus getStatus() {
    return this.status;
  }

  public void write(DataOutput out) throws IOException {
    out.writeLong(this.timestamp);
    out.writeInt(this.type.value());
    if (getType().value() == Directive.Type.Request.value()) {
      if (this.actions == null) {
        WritableUtils.writeVInt(out, 0);
      } else {
        WritableUtils.writeVInt(out, actions.length);
        for (GroomServerAction action : this.actions) {
          WritableUtils.writeEnum(out, action.getActionType());
          action.write(out);
        }
      }
      String[] groomServerNames = groomServerPeers.keySet().toArray(
          new String[0]);
      WritableUtils.writeCompressedStringArray(out, groomServerNames);

      List<String> groomServerAddresses = new ArrayList<String>(
          groomServerNames.length);
      for (String groomName : groomServerNames) {
        groomServerAddresses.add(groomServerPeers.get(groomName));
      }
      WritableUtils.writeCompressedStringArray(out, groomServerAddresses
          .toArray(new String[0]));
    } else if (getType().value() == Directive.Type.Response.value()) {
      this.status.write(out);
    } else {
      throw new IllegalStateException("Wrong directive type:" + getType());
    }

  }

  public void readFields(DataInput in) throws IOException {
    this.timestamp = in.readLong();
    int t = in.readInt();
    if (Directive.Type.Request.value() == t) {
      this.type = Directive.Type.Request;
      int length = WritableUtils.readVInt(in);
      if (length > 0) {
        this.actions = new GroomServerAction[length];
        for (int i = 0; i < length; ++i) {
          GroomServerAction.ActionType actionType = WritableUtils.readEnum(in,
              GroomServerAction.ActionType.class);
          actions[i] = GroomServerAction.createAction(actionType);
          actions[i].readFields(in);
        }
      } else {
        this.actions = null;
      }
      String[] groomServerNames = WritableUtils.readCompressedStringArray(in);
      String[] groomServerAddresses = WritableUtils
          .readCompressedStringArray(in);
      groomServerPeers = new HashMap<String, String>(groomServerNames.length);

      for (int i = 0; i < groomServerNames.length; i++) {
        groomServerPeers.put(groomServerNames[i], groomServerAddresses[i]);
      }
    } else if (Directive.Type.Response.value() == t) {
      this.type = Directive.Type.Response;
      this.status = new GroomServerStatus();
      this.status.readFields(in);
    } else {
      throw new IllegalStateException("Wrong directive type:" + t);
    }
  }
}
