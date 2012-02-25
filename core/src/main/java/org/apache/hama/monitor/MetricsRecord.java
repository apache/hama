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
package org.apache.hama.monitor;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.List;

/**
 * Represents a record containing multiple metrics. 
 */
public final class MetricsRecord  {

  private final String name, description;
  private final List<Metric<? extends Number>> metrics = 
    new CopyOnWriteArrayList<Metric<? extends Number>>();
  private final List<MetricsTag> tags = new CopyOnWriteArrayList<MetricsTag>();

  public MetricsRecord(String name, String description){
    this.name = name;
    this.description = description;
  }

  public MetricsRecord(String name){
    this(name, name+" record."); 
  }

  public final String name(){
    return name;
  }

  public final String description(){
    return description;
  }

  public final void add(Metric<? extends Number> metric){
    metrics.add(metric);
  }

  public final void add(List<Metric<? extends Number>> metrics){
    metrics.addAll(metrics);
  }

  public final void tag(Enum key, String value){
    tags.add(new MetricsTag(key.toString(), value));
  }

  public final void tag(String key, String value){
    tags.add(new MetricsTag(key, value));
  }

  public final Collection<MetricsTag> tags(){
    return Collections.unmodifiableList(tags);
  }

  public final List<Metric<? extends Number>> metrics(){
    return Collections.unmodifiableList(metrics);
  }
}
