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
package org.apache.hama.metrics;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public abstract class AbstractPatternFilter implements MetricsFilter {

  public static final Log LOG = LogFactory.getLog(AbstractPatternFilter.class);
  
  private final Pattern includePattern;
  private final Pattern excludePattern;
  private final Map<String, Pattern> includeTagPatterns;
  private final Map<String, Pattern> excludeTagPatterns;
  private final Pattern tagPattern = Pattern.compile("^(\\w+):(.*)");
  private static final char BACKSLASH = '\\';

  public AbstractPatternFilter(Set<MetricsConfig.Entry<String, String>> set) { 
    Pattern in = null, ex = null;
    Map<String, Pattern> inTags = new ConcurrentHashMap<String, Pattern>();
    Map<String, Pattern> exTags = new ConcurrentHashMap<String, Pattern>();
    for(MetricsConfig.Entry<String, String> e: set){
      String key = e.key();
      String value = e.value();
      if(LOG.isDebugEnabled()){
        LOG.info("Subset content of MetricsConfig's key "+key+" value "+value);
      }
      // data read from properties files should pay attention to excape sequence
      if(key.matches("(.*)(\\.include)")){
        in = compile(value);
      }else if(key.matches("(.*)(\\.exclude)")){
        ex = compile(value);
      }

      if(key.matches("(.*)(\\.include.tags)")){
        if(null != value && !value.isEmpty()) {
          Matcher m = tagPattern.matcher(value);
          if(m.matches()){
            inTags.put(m.group(1), compile(m.group(2))); 
          }
        }
      }else if(key.matches("(.*)(\\.exclude.tags)")){
         if(null != value && !value.isEmpty()) {
          Matcher m = tagPattern.matcher(value);
          if(m.matches()){
            exTags.put(m.group(1), compile(m.group(2))); 
          }
        }
      }
    }
    this.includePattern = in;
    this.excludePattern = ex;
    this.includeTagPatterns = inTags;
    this.excludeTagPatterns = exTags;
  }

  /**
   * Return true if include pattern matches the name; false if exclude pattern 
   * matches the name. Also, if include pattern is provided but no exclude 
   * pattern, name won't be accepted as it is white list only mode. 
   * @return ture if accepted; false otherwise.
   */
  @Override
  public boolean accepts(String name) {
    if (includePattern != null && includePattern.matcher(name).matches()) {
      return true;
    }
    if ((excludePattern != null && excludePattern.matcher(name).matches())) {
      return false;
    }
    // Reject if no matches in white list only mode.
    if (includePattern != null && excludePattern == null) {
      return false;
    }
    return true;
  }

  /**
   * True if tags are listed as include tag pattern; false if tags
   * are listed in exclude tag pattern. If include and exclude tags pattern
   * are empty, accepts returns false. 
   * @param tags to be tested if accepted in filter.
   * @return boolean value tells if the filter accepts the tag.
   */
  @Override
  public boolean accepts(Iterable<MetricsTag> tags) {
    // Accept if any include tag pattern matches
    for (MetricsTag t : tags) {
      Pattern pat = includeTagPatterns.get(t.name());
      if (pat != null && pat.matcher(t.value()).matches()) {
        return true;
      }
    }
    // Reject if any exclude tag pattern matches
    for (MetricsTag t : tags) {
      Pattern pat = excludeTagPatterns.get(t.name());
      if (pat != null && pat.matcher(t.value()).matches()) {
        return false;
      }
    }
    // Reject if no match in whitelist only mode
    if (!includeTagPatterns.isEmpty() && excludeTagPatterns.isEmpty()) {
      return false;
    }
    return true;
  }

  /**
   * True if the record is accepted; false otherwise.
   * @param record to filter on
   * @return  true to accept; false otherwise.
   */
  public boolean accepts(MetricsRecord record) {
    return accepts(record.tags());
  }

  public abstract Pattern compile(String glob); 

}
