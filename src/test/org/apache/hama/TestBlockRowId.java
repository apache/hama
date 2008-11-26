/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hama;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hama.io.BlockID;
import org.apache.hama.mapred.BlockingMapRed.BlockRowId;

import junit.framework.TestCase;

public class TestBlockRowId extends TestCase {

  static final Log LOG = 
    LogFactory.getLog(TestBlockRowId.class);
  
  DataOutputBuffer outBuf = new DataOutputBuffer();
  DataInputBuffer inBuf = new DataInputBuffer();
  
  public void testIO() throws IOException {
    BlockRowId id1 = new BlockRowId(new BlockID(1, 2), 1);
    outBuf.reset();
    id1.write(outBuf);
    
    inBuf.reset(outBuf.getData(), outBuf.getLength());
    
    BlockRowId id2 = new BlockRowId();
    id2.readFields(inBuf);
    
    assertEquals(0, id1.compareTo(id2));
  }
  
}
