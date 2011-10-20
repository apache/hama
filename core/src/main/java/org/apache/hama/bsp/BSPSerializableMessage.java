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
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.io.Writable;

public class BSPSerializableMessage implements Writable {
  final AtomicReference<String> path = new AtomicReference<String>();
  final AtomicReference<BSPMessageBundle> bundle = new AtomicReference<BSPMessageBundle>();

  public BSPSerializableMessage() {
  }

  public BSPSerializableMessage(final String path, final BSPMessageBundle bundle) {
    if (null == path)
      throw new NullPointerException("No path provided for checkpointing.");
    if (null == bundle)
      throw new NullPointerException("No data provided for checkpointing.");
    this.path.set(path);
    this.bundle.set(bundle);
  }

  public final String checkpointedPath() {
    return this.path.get();
  }

  public final BSPMessageBundle messageBundle() {
    return this.bundle.get();
  }

  @Override
  public final void write(DataOutput out) throws IOException {
    out.writeUTF(this.path.get());
    this.bundle.get().write(out);
  }

  @Override
  public final void readFields(DataInput in) throws IOException {
    this.path.set(in.readUTF());
    BSPMessageBundle pack = new BSPMessageBundle();
    pack.readFields(in);
    this.bundle.set(pack);
  }
}
