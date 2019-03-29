/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.ptest.execution;

import java.io.File;

import org.junit.Assert;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;
import com.google.common.io.Files;

public class TestReportParser {
  private static final Logger LOG = LoggerFactory
      .getLogger(TestReportParser.class);
  private File baseDir;
  @Before
  public void setup() {
    baseDir = Files.createTempDir();
  }
  @After
  public void teardown() {
    if(baseDir != null) {
      FileUtils.deleteQuietly(baseDir);
    }
  }
  @Test
  public void test() throws Exception {
    File reportDir = new File("src/test/resources/test-outputs");
    for(File file : reportDir.listFiles()) {
      if(file.isFile()) {
        if(file.getName().endsWith(".xml")) {
          Files.copy(file, new File(baseDir, "TEST-" + file.getName()));
        } else {
          Files.copy(file, new File(baseDir, file.getName()));
        }
      }
    }
    JUnitReportParser parser = new JUnitReportParser(LOG, baseDir);
    Assert.assertEquals(3, parser.getAllFailedTests().size());
    Assert.assertEquals(Sets.
        newHashSet("org.apache.hadoop.hive.cli.TestCliDriver.testCliDriver_skewjoin_union_remove_1",
            "org.apache.hadoop.hive.cli.TestCliDriver.testCliDriver_union_remove_9",
            "org.apache.hadoop.hive.cli.TestCliDriver.testCliDriver_skewjoin"),
            parser.getAllFailedTests());
    Assert.assertEquals(Sets.
        newHashSet("org.apache.hadoop.hive.cli.TestCliDriver.testCliDriver_shutdown", 
            "org.apache.hadoop.hive.cli.TestCliDriver.testCliDriver_binary_constant", 
            "org.apache.hadoop.hive.cli.TestCliDriver.testCliDriver_skewjoin_union_remove_1", 
            "org.apache.hadoop.hive.cli.TestCliDriver.testCliDriver_udf_regexp_extract", 
            "org.apache.hadoop.hive.cli.TestCliDriver.testCliDriver_index_auth", 
            "org.apache.hadoop.hive.cli.TestCliDriver.testCliDriver_auto_join17", 
            "org.apache.hadoop.hive.cli.TestCliDriver.testCliDriver_authorization_2", 
            "org.apache.hadoop.hive.cli.TestCliDriver.testCliDriver_load_dyn_part3", 
            "org.apache.hadoop.hive.cli.TestCliDriver.testCliDriver_index_bitmap2", 
            "org.apache.hadoop.hive.cli.TestCliDriver.testCliDriver_groupby_rollup1", 
            "org.apache.hadoop.hive.cli.TestCliDriver.testCliDriver_bucketcontext_3", 
            "org.apache.hadoop.hive.cli.TestCliDriver.testCliDriver_ppd_join", 
            "org.apache.hadoop.hive.cli.TestCliDriver.testCliDriver_rcfile_lazydecompress", 
            "org.apache.hadoop.hive.cli.TestCliDriver.testCliDriver_notable_alias1", 
            "org.apache.hadoop.hive.cli.TestCliDriver.testCliDriver_union_remove_9", 
            "org.apache.hadoop.hive.cli.TestCliDriver.testCliDriver_skewjoin", 
            "org.apache.hadoop.hive.cli.TestCliDriver.testCliDriver_multi_insert_gby"),
            parser.getAllExecutedTests());
  }
}
