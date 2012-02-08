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
package org.apache.hama.bsp.message;

public interface Sender {
  public static final org.apache.avro.Protocol PROTOCOL = org.apache.avro.Protocol
      .parse("{\"protocol\":\"Sender\",\"namespace\":\"de.jungblut.avro\",\"types\":[{\"type\":\"record\",\"name\":\"AvroBSPMessageBundle\",\"fields\":[{\"name\":\"data\",\"type\":\"bytes\"}]}],\"messages\":{\"transfer\":{\"request\":[{\"name\":\"messagebundle\",\"type\":\"AvroBSPMessageBundle\"}],\"response\":\"null\"}}}");

  java.lang.Void transfer(AvroBSPMessageBundle messagebundle)
      throws org.apache.avro.AvroRemoteException;

  @SuppressWarnings("all")
  public interface Callback extends Sender {
    public static final org.apache.avro.Protocol PROTOCOL = Sender.PROTOCOL;

    void transfer(AvroBSPMessageBundle messagebundle,
        org.apache.avro.ipc.Callback<java.lang.Void> callback)
        throws java.io.IOException;
  }
}
