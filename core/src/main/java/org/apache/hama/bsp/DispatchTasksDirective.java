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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 * Handles the tasks dispatching between the BSPMaster and the GroomServers.
 */
public final class DispatchTasksDirective extends Directive implements Writable {

  public static final Log LOG = LogFactory.getLog(DispatchTasksDirective.class);

  private GroomServerAction[] actions;

  public DispatchTasksDirective() {
    super();
  }

  public DispatchTasksDirective(GroomServerAction[] actions) {
    super(Directive.Type.Request);
    this.actions = actions;
  }

  public GroomServerAction[] getActions() {
    return this.actions;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    if (this.actions == null) {
      WritableUtils.writeVInt(out, 0);
    } else {
      WritableUtils.writeVInt(out, actions.length);
      for (GroomServerAction action : this.actions) {
        WritableUtils.writeEnum(out, action.getActionType());
        action.write(out);
      }
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
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
  }
}
