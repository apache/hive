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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class TestJavaVersionUtils {

  @Test
  public void testAddOpensFlagsAppendedToMRJobOpts() {
    Configuration job = new Configuration(false);
    job.set("mapreduce.map.java.opts", "-Xmx800m");

    JavaVersionUtils.addOpensFlags(job);

    String flags = JavaVersionUtils.getAddOpensFlags();
    assertTrue(flags.contains("--add-opens=java.base/java.net=ALL-UNNAMED"));
    // existing options are kept and the flags appended; unset keys get just the flags
    assertEquals("-Xmx800m" + flags, job.get("mapreduce.map.java.opts"));
    assertEquals(flags, job.get("mapreduce.reduce.java.opts"));
    assertEquals(flags, job.get("yarn.app.mapreduce.am.command-opts"));
  }
}
