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
package org.apache.hama.monitor.fd;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.hama.HamaConfiguration;

public final class FDProvider {

  private static final ConcurrentMap<Class<? extends Supervisor>, Object> supervisorCache = new ConcurrentHashMap<Class<? extends Supervisor>, Object>();
  private static final ConcurrentMap<Class<? extends Sensor>, Object> sensorCache = new ConcurrentHashMap<Class<? extends Sensor>, Object>();

  public static Supervisor createSupervisor(Class<? extends Supervisor> key,
      HamaConfiguration conf) {
    Supervisor supervisor = (Supervisor) supervisorCache.get(key);
    if (null == supervisor) {
      supervisor = new UDPSupervisor(conf);
      Supervisor old = (Supervisor) supervisorCache
          .putIfAbsent(key, supervisor);
      if (null != old) {
        supervisor = old;
      }
    }
    return supervisor;
  }

  public static Sensor createSensor(Class<? extends Sensor> key,
      HamaConfiguration conf) {
    Sensor sensor = (Sensor) sensorCache.get(key);
    if (null == sensor) {
      sensor = new UDPSensor(conf);
      Sensor old = (Sensor) sensorCache.putIfAbsent(key, sensor);
      if (null != old) {
        sensor = old;
      }
    }
    return sensor;
  }

}
