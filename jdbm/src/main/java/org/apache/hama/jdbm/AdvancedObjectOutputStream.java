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
package org.apache.hama.jdbm;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.OutputStream;
import java.util.ArrayList;

import org.apache.hama.jdbm.SerialClassInfo.ClassInfo;

/**
 * An alternative to <code>java.io.ObjectOutputStream</code> which uses more
 * efficient serialization
 */
public final class AdvancedObjectOutputStream extends DataOutputStream
    implements ObjectOutput {

  public AdvancedObjectOutputStream(OutputStream out) {
    super(out);
  }

  @Override
  public void writeObject(Object obj) throws IOException {
    ArrayList<ClassInfo> registered = new ArrayList<ClassInfo>();
    Serialization ser = new Serialization(null, 0, registered);

    byte[] data = ser.serialize(obj);
    // write class info first
    SerialClassInfo.serializer.serialize(this, registered);
    // and write data
    write(data);
  }
}
