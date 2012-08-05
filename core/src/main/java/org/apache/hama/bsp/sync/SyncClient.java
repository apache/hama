/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hama.bsp.sync;

import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hama.bsp.BSPJobID;

/**
 * Basic interface for a client that connects to a sync server.
 * 
 */
public interface SyncClient {

  /**
   * Construct key in the format required by the SyncClient for storing and 
   * retrieving information. This function is recommended to use to construct
   * keys for storing keys.
   * @param jobId The BSP Job Id.
   * @param args The list of String objects that would be used to construct key
   * @return The key consisting of entities provided in the required format.
   */
  public String constructKey(BSPJobID jobId, String ... args);

  /**
   * Stores value for the specified key.
   * @param key The key for which value should be stored. It is recommended to use 
   * <code>constructKey</code> to create key object.
   * @param value The value to be stored.
   * @param permanent true if the value should be persisted after end of session.
   * @param Listener object that provides asynchronous updates on the state 
   * of information stored under the key.
   * @return true if the operation was successful.
   */
  public boolean storeInformation(String key, Writable value, 
      boolean permanent, SyncEventListener listener);

  /**
   * Retrieve value previously store for the key.
   * @param key The key for which value was stored.
   * @param classType The expected class instance of value to be extracted
   * @return the value if found. Returns null if there was any error of if there
   * was no value stored for the key.
   */
  public boolean getInformation(String key, Writable valueHolder);

  /**
   * Store new key in key set.
   * @param key The key to be saved in key set. It is recommended to use 
   * <code>constructKey</code> to create key object. 
   * @param permanent true if the value should be persisted after end of session.
   * @param listener Listener object that asynchronously notifies the events 
   * related to the key.
   * @return true if operation was successful.
   */
  public boolean addKey(String key, boolean permanent, SyncEventListener listener);

  /**
   * Check if key was previously stored.
   * @param key The value of the key. 
   * @return true if the key exists.
   */
  public boolean hasKey(String key);
  
  /**
  * Get list of child keys stored under the key provided.
  * @param key The key whose child key set are to be found.
  * @param listener Listener object that asynchronously notifies the changes 
  * under the provided key
  * @return Array of child keys.
  */
  public String[] getChildKeySet(String key, SyncEventListener listener);

  /**
   * Register a listener for events on the key.
   * @param key The key on which an event listener should be registered.
   * @param event for which the listener is registered for.
   * @param listener The event listener that defines how to process the event.
   * @return true if the operation is successful.
   */
  public boolean registerListener(String key, SyncEvent event,
      SyncEventListener listener);

  /**
   * Delete the key and the information stored under it.
   * @param key
   * @param listener
   * @return
   */
  public boolean remove(String key, SyncEventListener listener);
  
  /**
   * 
   */
  public void close() throws IOException;
  
}
