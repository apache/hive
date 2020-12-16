/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hive.service.testutil;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.List;

public class TestHiveQueriesSample {

  private static TestHiveShell shell;

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @BeforeClass
  public static void beforeClass() {
    shell = new TestHiveShell();
    shell.setHiveConfValue("hive.notification.event.poll.interval", "-1");
    shell.setHiveConfValue("hive.tez.exec.print.summary", "true");
    shell.start();
  }

  @AfterClass
  public static void afterClass() {
    shell.stop();
  }

  @Before
  public void before() {
    shell.openSession();
    shell.setHiveSessionValue("hive.jar.directory", temp.getRoot().getAbsolutePath());
    shell.setHiveSessionValue("tez.staging-dir", temp.getRoot().getAbsolutePath());
  }

  @After
  public void after() throws Exception {
    shell.closeSession();
    shell.metastore().reset();
  }

  @Test
  public void testScanEmptyTable() {
    shell.executeStatement("CREATE TABLE empty (a int, b string)");
    List<Object[]> rows = shell.executeStatement("SELECT * FROM default.empty");
    Assert.assertEquals(0, rows.size());
  }

  @Test
  public void testInsertAndReadOrderByNonAcid() {
    shell.executeStatement("CREATE TABLE abc (a int, b string) TBLPROPERTIES (\"transactional\"=\"false\")");
    shell.executeStatement("INSERT INTO abc VALUES (10, 'some'), (20, 'more')");
    List<Object[]> rows = shell.executeStatement("SELECT * FROM default.abc ORDER BY a DESC");
    Assert.assertEquals(2, rows.size());
    Assert.assertEquals(new Object[]{ 20, "more" }, rows.get(0));
    Assert.assertEquals(new Object[]{ 10, "some" }, rows.get(1));
  }

  @Test
  public void testInsertAndReadOrderByAcid() {
    shell.executeStatement("CREATE TABLE abc (a int, b string) TBLPROPERTIES (\"transactional\"=\"true\")");
    shell.executeStatement("INSERT INTO abc VALUES (10, 'some'), (20, 'more')");
    List<Object[]> rows = shell.executeStatement("SELECT * FROM default.abc ORDER BY a DESC");
    Assert.assertEquals(2, rows.size());
    Assert.assertEquals(new Object[]{ 20, "more" }, rows.get(0));
    Assert.assertEquals(new Object[]{ 10, "some" }, rows.get(1));
  }
}
