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
package org.apache.hive.hcatalog.templeton.tool;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import junit.framework.Assert;

public class TestJobIDParser {
  @Test
  public void testParsePig() throws IOException {
    String errFileName = "src/test/data/status/pig";
    PigJobIDParser pigJobIDParser = new PigJobIDParser(errFileName, new Configuration());
    List<String> jobs = pigJobIDParser.parseJobID();
    Assert.assertEquals(jobs.size(), 1);
  }

  @Test
  public void testParseHive() throws IOException {
    String errFileName = "src/test/data/status/hive";
    HiveJobIDParser hiveJobIDParser = new HiveJobIDParser(errFileName, new Configuration());
    List<String> jobs = hiveJobIDParser.parseJobID();
    Assert.assertEquals(jobs.size(), 1);
  }

  @Test
  public void testParseJar() throws IOException {
    String errFileName = "src/test/data/status/jar";
    JarJobIDParser jarJobIDParser = new JarJobIDParser(errFileName, new Configuration());
    List<String> jobs = jarJobIDParser.parseJobID();
    Assert.assertEquals(jobs.size(), 1);
  }

  @Test
  public void testParseStreaming() throws IOException {
    String errFileName = "src/test/data/status/streaming";
    JarJobIDParser jarJobIDParser = new JarJobIDParser(errFileName, new Configuration());
    List<String> jobs = jarJobIDParser.parseJobID();
    Assert.assertEquals(jobs.size(), 1);
  }

}
