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
package org.apache.hama.manager;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hama.manager.util.UITemplate;

public class TestLogView extends TestCase {

  public static final Log LOG = LogFactory.getLog(UITemplate.class);

  public void testconvertFileSize() throws Exception {

    assertEquals("File size 1024.", LogView.convertFileSize(1024), "1 KB");
    assertEquals("File size 1048576.", LogView.convertFileSize(1048576), "1 MB");
    assertEquals("File size 1073741824.", LogView.convertFileSize(1073741824),
        "1 GB");
    assertEquals("File size 1099511627776L.",
        LogView.convertFileSize(1099511627776L), "1 TB");
    assertEquals("File size 0.", LogView.convertFileSize(0), "0 bytes");
  }
}
