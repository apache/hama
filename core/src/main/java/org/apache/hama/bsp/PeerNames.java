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
import java.util.Collection;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * 
 */
public class PeerNames implements Writable {
  Collection<String> allPeers;
  
  public PeerNames() {
    this.allPeers = new ArrayList<String>();
  }
  
  public PeerNames(Collection<String> allPeers) {
    this.allPeers = allPeers;
  }
  
  public Collection<String> getAllPeerNames() {
    return allPeers;
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(allPeers.size());
    for (String peerName : allPeers) {
      Text.writeString(out, peerName);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int peersNum = in.readInt();
    for (int i = 0; i < peersNum; i++) {
      allPeers.add(Text.readString(in));
    }
  }

}
