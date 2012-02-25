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

public final class Metric<T> {

  private final String name;
  private final String description;
  private final T value;

  public Metric(Enum name, T value){
    this(name.name(), name.name()+" metric.", value);
  }

  public Metric(String name, T value){
    this(name, name+" metric.", value);
  }

  public Metric(String name, String description, T value){
    this.name = name;
    this.description = description;
    this.value = value;
  }

  public final String name(){
    return this.name;
  } 

  public final String description(){
    return this.description;
  }

  public final T value(){
    return this.value;
  }

  @Override
  public boolean equals(Object target){
   if (target == this)
      return true;
    if (null == target)
      return false;
    if (getClass() != target.getClass())
      return false;

    Metric m = (Metric) target;
    if(!name().equals(m.name))
      return false;
    if(!description().equals(m.description))
      return false;

    return true;
  }

  @Override 
  public int hashCode(){
    int result = 17;
    result = 37 * result + name.hashCode();
    result = 37 * result + description.hashCode();
    return result;
  }

}
