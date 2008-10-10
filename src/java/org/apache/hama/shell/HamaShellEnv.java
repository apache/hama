/*
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
package org.apache.hama.shell;

import java.util.HashMap;
import java.util.Map;
/**
 * HamaShellEnv.
 * 
 * Maintain all the aliases in the HamaShell.
 * 
 */
// TODO : 
//
// we need other data structures to maintain other informations
// (such as: operators, common operations, data dependency)
// so that we can use these information to rearrange the expressions
// and make the hama shell executed more efficiently and parallel.
public class HamaShellEnv {

  Map<String, Object> aliases = new HashMap<String, Object>();
  
  public static final int DEFAULT_MAP_NUM = 2;
  public static final int DEFAULT_REDUCE_NUM = 1;
  
  public Object getAliase(String var) {
    return aliases.get(var);
  }
  
  public void setAliase(String aliaseName, Object value) {
    if(aliases.containsKey(aliaseName)) {
      aliases.remove(aliaseName);
    }
    aliases.put(aliaseName, value); 
  }
  
  public boolean containAliase(String aliaseName) {
    return aliases.containsKey(aliaseName);
  }
  
  public Map<String, Object> getAliases() {
    return aliases;
  }
  
}
