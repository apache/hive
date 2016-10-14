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
package org.apache.hive.ptest.execution.conf;

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Joiner;
import com.google.common.collect.Sets;

public class TestQFileTestBatch {

  private static final String DRIVER = "driver";
  private static final String QUERY_FILES_PROPERTY = "qfile";
  private static final String TEST_MODULE_NAME = "testModule";

  private Set<String> tests;

  @Before
  public void setup() {
    tests = Sets.newTreeSet(Sets.newHashSet("a", "b", "c"));
  }

  @Test
  public void testParallel() throws Exception {
    QFileTestBatch batch =
        new QFileTestBatch(new AtomicInteger(1), "testcase", DRIVER, QUERY_FILES_PROPERTY, tests, true, TEST_MODULE_NAME);
    Assert.assertTrue(batch.isParallel());
    Assert.assertEquals(DRIVER, batch.getDriver());
    Assert.assertEquals(Joiner.on("-").join("1", DRIVER, "a", "b", "c"), batch.getName());
    Assert.assertEquals(String.format("-Dtestcase=%s -D%s=a,b,c", DRIVER,
        QUERY_FILES_PROPERTY), batch.getTestArguments());
    Assert.assertEquals(TEST_MODULE_NAME, batch.getTestModuleRelativeDir());
  }
  @Test
  public void testMoreThanThreeTests() throws Exception {
    Assert.assertTrue(tests.add("d"));
    QFileTestBatch batch =
        new QFileTestBatch(new AtomicInteger(1), "testcase", DRIVER, QUERY_FILES_PROPERTY, tests, true, TEST_MODULE_NAME);
    Assert.assertEquals(Joiner.on("-").join("1", DRIVER, "a", "b", "c", "and", "1", "more"),
        batch.getName());
  }
  @Test
  public void testNotParallel() throws Exception {
    QFileTestBatch batch =
        new QFileTestBatch(new AtomicInteger(1), "testcase", DRIVER, QUERY_FILES_PROPERTY, tests, false,
            TEST_MODULE_NAME);
    Assert.assertFalse(batch.isParallel());
  }
}
