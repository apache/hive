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

package org.apache.hive.service.cli.session;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.Before;
import org.junit.Test;

public class TestPersistableSessionUtils {

  private SessionState sessionState;

  @Before
  public void setUp() {
    sessionState = new SessionState(new HiveConf());
    sessionState.setCurrentDatabase("default");
    Map<String, Table> tempTables = new HashMap<>();
    tempTables.put("tmp", new Table("default", "tmp"));
    sessionState.getTempTables().put("default", tempTables);
  }

  @Test
  public void testShouldPersistSnapshotForInsertIntoTempTable() {
    assertTrue(PersistableSessionUtils.shouldPersistSnapshot(
        "INSERT INTO tmp PARTITION(dt='2024-01-01') VALUES (1)", sessionState));
    assertTrue(PersistableSessionUtils.shouldPersistSnapshot(
        "insert overwrite table tmp partition (dt='x') select 1", sessionState));
    assertTrue(PersistableSessionUtils.shouldPersistSnapshot(
        "LOAD DATA INPATH '/tmp/data' INTO TABLE tmp PARTITION (dt='x')", sessionState));
  }

  @Test
  public void testShouldNotPersistSnapshotForDmlOnPersistentTable() {
    assertFalse(PersistableSessionUtils.shouldPersistSnapshot(
        "INSERT INTO permanent_table VALUES (1)", sessionState));
    assertFalse(PersistableSessionUtils.shouldPersistSnapshot(
        "INSERT OVERWRITE TABLE permanent_table SELECT 1", sessionState));
    assertFalse(PersistableSessionUtils.shouldPersistSnapshot(
        "LOAD DATA INPATH '/tmp/data' INTO TABLE permanent_table", sessionState));
  }

  @Test
  public void testShouldPersistSnapshotForStateChangingCommands() {
    assertTrue(PersistableSessionUtils.shouldPersistSnapshot(
        "SET hive.exec.mode=strict", sessionState));
    assertTrue(PersistableSessionUtils.shouldPersistSnapshot(
        "CREATE TEMPORARY TABLE tmp (id INT)", sessionState));
    assertFalse(PersistableSessionUtils.shouldPersistSnapshot("SELECT 1", sessionState));
  }
}
