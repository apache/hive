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
package org.apache.hadoop.hive.ql;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Assert;
import org.junit.Test;

/**
 * This class contains unit tests for QTestUtil
 */
public class TestQTestUtil {
  private static final String TEST_HDFS_MASK = "###HDFS###";
  private static final String TEST_HDFS_DATE_MASK = "###HDFS_DATE###";
  private static final String TEST_HDFS_USER_MASK = "###USER###";
  private static final String TEST_HDFS_GROUP_MASK = "###GROUP###";

  @Test
  public void testSelectiveHdfsPatternMaskOnlyHdfsPath() {
    Assert.assertEquals("nothing to be masked", maskHdfs("nothing to be masked"));
    Assert.assertEquals("hdfs://", maskHdfs("hdfs://"));
    Assert.assertEquals(String.format("hdfs://%s", TEST_HDFS_MASK), maskHdfs("hdfs://a"));
    Assert.assertEquals(String.format("hdfs://%s other text", TEST_HDFS_MASK),
        maskHdfs("hdfs://tmp.dfs.com:50029/tmp other text"));
    Assert.assertEquals(String.format("hdfs://%s", TEST_HDFS_MASK), maskHdfs(
        "hdfs://localhost:51594/build/ql/test/data/warehouse/default/encrypted_table_dp/p=2014-09-23"));

    String line = maskHdfs("hdfs://localhost:11111/tmp/ct_noperm_loc_foo1");
    Assert.assertEquals(String.format("hdfs://%s", TEST_HDFS_MASK), line);

    line = maskHdfs("hdfs://one hdfs://two");
    Assert.assertEquals(String.format("hdfs://%s hdfs://%s", TEST_HDFS_MASK, TEST_HDFS_MASK), line);

    line = maskHdfs(
        "some text before [name=hdfs://localhost:11111/tmp/ct_noperm_loc_foo1]] some text between hdfs://localhost:22222/tmp/ct_noperm_loc_foo2 some text after");
    Assert.assertEquals(String.format(
        "some text before [name=hdfs://%s]] some text between hdfs://%s some text after",
        TEST_HDFS_MASK, TEST_HDFS_MASK), line);

    line = maskHdfsWithDateUserGroup(
        "-rw-r--r--   3 hiveptest supergroup       2557 2018-01-11 17:09 hdfs://hello_hdfs_path");
    Assert.assertEquals(String.format("-rw-r--r--   3 %s %s       2557 %s hdfs://%s",
        TEST_HDFS_USER_MASK, TEST_HDFS_GROUP_MASK, TEST_HDFS_DATE_MASK, TEST_HDFS_MASK), line);

    line = maskHdfs(maskHdfsWithDateUserGroup(
        "-rw-r--r--   3 hiveptest supergroup       2557 2018-01-11 17:09 hdfs://hello_hdfs_path"));
    Assert.assertEquals(String.format("-rw-r--r--   3 %s %s       2557 %s hdfs://%s",
        TEST_HDFS_USER_MASK, TEST_HDFS_GROUP_MASK, TEST_HDFS_DATE_MASK, TEST_HDFS_MASK), line);
  }

  private String maskHdfs(String line) {
    Matcher matcher = Pattern.compile(QTestUtil.PATH_HDFS_REGEX).matcher(line);

    if (matcher.find()) {
      line = matcher.replaceAll(String.format("$1%s", TEST_HDFS_MASK));
    }

    return line;
  }

  private String maskHdfsWithDateUserGroup(String line) {
    Matcher matcher = Pattern.compile(QTestUtil.PATH_HDFS_WITH_DATE_USER_GROUP_REGEX).matcher(line);

    if (matcher.find()) {
      line = matcher.replaceAll(String.format("%s %s$3$4 %s $6%s", TEST_HDFS_USER_MASK,
          TEST_HDFS_GROUP_MASK, TEST_HDFS_DATE_MASK, TEST_HDFS_MASK));
    }

    return line;
  }
}