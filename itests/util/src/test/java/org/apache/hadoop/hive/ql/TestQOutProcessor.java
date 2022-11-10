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
package org.apache.hadoop.hive.ql;

import org.apache.hadoop.hive.ql.QTestMiniClusters.FsType;
import org.apache.hadoop.hive.ql.qoption.QTestReplaceHandler;
import org.junit.Assert;
import org.junit.Test;

/**
 * This class contains unit tests for QTestUtil
 */
public class TestQOutProcessor {
  QOutProcessor qOutProcessor = new QOutProcessor(FsType.LOCAL, new QTestReplaceHandler());

  @Test
  public void testSelectiveHdfsPatternMaskOnlyHdfsPath() {
    Assert.assertEquals("nothing to be masked", processLine("nothing to be masked"));
    Assert.assertEquals("hdfs://", processLine("hdfs://"));
    Assert.assertEquals(String.format("hdfs://%s", QOutProcessor.HDFS_MASK),
        processLine("hdfs:///"));
    Assert.assertEquals(String.format("hdfs://%s", QOutProcessor.HDFS_MASK),
        processLine("hdfs://a"));
    Assert.assertEquals(String.format("hdfs://%s other text", QOutProcessor.HDFS_MASK),
        processLine("hdfs://tmp.dfs.com:50029/tmp other text"));
    Assert.assertEquals(String.format("hdfs://%s", QOutProcessor.HDFS_MASK), processLine(
        "hdfs://localhost:51594/build/ql/test/data/warehouse/default/encrypted_table_dp/p=2014-09-23"));

    Assert.assertEquals(String.format("hdfs://%s", QOutProcessor.HDFS_MASK),
        processLine("hdfs://localhost:11111/tmp/ct_noperm_loc_foo1"));

    Assert.assertEquals(
        String.format("hdfs://%s hdfs://%s", QOutProcessor.HDFS_MASK, QOutProcessor.HDFS_MASK),
        processLine("hdfs://one hdfs://two"));

    Assert.assertEquals(
        String.format(
            "some text before [name=hdfs://%s]] some text between hdfs://%s some text after",
            QOutProcessor.HDFS_MASK, QOutProcessor.HDFS_MASK),
        processLine(
            "some text before [name=hdfs://localhost:11111/tmp/ct_noperm_loc_foo1]] some text between hdfs://localhost:22222/tmp/ct_noperm_loc_foo2 some text after"));

    Assert.assertEquals(
        String.format("-rw-r--r--   3 %s %s       2557 %s hdfs://%s", QOutProcessor.HDFS_USER_MASK,
            QOutProcessor.HDFS_GROUP_MASK, QOutProcessor.HDFS_DATE_MASK, QOutProcessor.HDFS_MASK),
        processLine(
            "-rw-r--r--   3 hiveptest supergroup       2557 2018-01-11 17:09 hdfs://hello_hdfs_path"));

    Assert.assertEquals(
        String.format("-rw-r--r--   3 %s %s       2557 %s hdfs://%s", QOutProcessor.HDFS_USER_MASK,
            QOutProcessor.HDFS_GROUP_MASK, QOutProcessor.HDFS_DATE_MASK, QOutProcessor.HDFS_MASK),
        processLine(
            "-rw-r--r--   3 hiveptest supergroup       2557 2018-01-11 17:09 hdfs://hello_hdfs_path"));
  }

  private String processLine(String line) {
    return qOutProcessor.processLine(line).get();
  }
}