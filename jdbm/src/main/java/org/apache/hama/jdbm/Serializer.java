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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Interface used to provide a serialization mechanism other than a class'
 * normal serialization.
 */
public interface Serializer<A> {

  /**
   * Serialize the content of an object into a byte array.
   * 
   * @param out ObjectOutput to save object into
   * @param obj Object to serialize
   */
  public void serialize(DataOutput out, A obj) throws IOException;

  /**
   * Deserialize the content of an object from a byte array.
   * 
   * @param in to read serialized data from
   * @return deserialized object
   * @throws IOException
   * @throws ClassNotFoundException
   */
  public A deserialize(DataInput in) throws IOException, ClassNotFoundException;

}
