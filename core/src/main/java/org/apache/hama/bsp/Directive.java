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

import org.apache.hadoop.io.Writable;

/**
 * A generic directive from the {@link org.apache.hama.bsp.BSPMaster} to the
 * {@link org.apache.hama.bsp.GroomServer} to take some 'action'.
 */
public class Directive implements Writable{

  protected long timestamp;
  protected Directive.Type type;

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

  public Directive(){}

  public Directive(Directive.Type type) {
    this.timestamp = System.currentTimeMillis();
    this.type = type;
  }

  public long getTimestamp() {
    return this.timestamp;
  }

  public Directive.Type getType() {
    return this.type;
  }

  /**
   * Command for BSPMaster or GroomServer to execute.
  public abstract void execute() throws Exception;
   */

  public void write(DataOutput out) throws IOException {
    out.writeLong(this.timestamp);
    out.writeInt(this.type.value());
  }

  public void readFields(DataInput in) throws IOException {
    this.timestamp = in.readLong();
    int t = in.readInt();
    if (Directive.Type.Request.value() == t) {
      this.type = Directive.Type.Request;
    }else{
      this.type = Directive.Type.Response;
    }
  }
}
