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

package org.apache.hadoop.hive.ql.anon.cmd;

import org.apache.hadoop.hive.ql.anon.BaseTest;
import org.apache.hadoop.hive.ql.anon.builders.AlterIndexStatementBuilder;
import org.apache.hadoop.hive.ql.anon.builders.CreateIndexStatementBuilder;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class TestAlterIndexRebuild extends BaseTest {

  private static final String TABLE  = "t_orc_pol_air_" + Long.toString(System.nanoTime(), 36);
  private static final String INDEX  = "ix1";
  private static final String COLUMN = "b";
  private static final String POLICY = "tair_pol";

  private static final String POLICY_DSL =
      """
      VERSION v1
      IDENTITY userId TYPE INT
      SCHEMA TYPE STRING
      FOR SCHEMA 'user_info_v1'
          ERASE telephone
      """;

  @Override
  protected String[] managedErasurePolicies() {
    return new String[] { POLICY };
  }

  @BeforeAll
  public void setup() throws CommandProcessorException, IOException {
    tblName = TABLE;
    try { execute("drop identity index if exists " + INDEX + " on " + tblName); }
    catch (CommandProcessorException expected) {  }
    try { execute("drop table if exists %s"); }
    catch (CommandProcessorException expected) {  }
    execute("create table if not exists %s (m int, o bigint, b string) stored as orc");
    execute("insert into table %s values (1, 11, '{\"pid\": 1}'), (2, 12, '{\"pid\": 2}')");
    execute("insert into table %s values (3, 31, '{\"pid\": 3}'), (4, 42, '{\"pid\": 4}')");

    final Path policyFile = Files.createTempFile(POLICY + "_", ".erp");
    Files.write(policyFile, POLICY_DSL.getBytes());
    execute("LOAD ERASURE POLICY " + POLICY + " FROM '" + policyFile + "'");
    execute("VALIDATE ERASURE POLICY " + POLICY);
    execute("ACTIVATE ERASURE POLICY " + POLICY);
    execute(("ATTACH DATA ERASURE POLICY %s ON TABLE %s COLUMN %s"
        + " WITH ( SCHEMA FIELD (m), ROW LOCATOR (o), COLUMN FORMAT (JSON) )"
        + " RESOLUTION ( EXPLICIT )").formatted(POLICY, tblName, COLUMN));

    final String createIx = new CreateIndexStatementBuilder(INDEX, tblName, COLUMN)
        .withBtreeOptions(8192, 10, "int").build();
    execute(createIx);
  }

  @Test
  public void testAlterIndex() throws CommandProcessorException {
    final String cmd = new AlterIndexStatementBuilder(INDEX, tblName).build();
    List<Object> lst = execute(cmd);
    Assertions.assertEquals(0, lst.size());
  }

  @AfterAll
  public void teardown() throws CommandProcessorException {
    try { execute("DROP IDENTITY INDEX IF EXISTS " + INDEX + " ON " + tblName); }
    catch (CommandProcessorException expected) {  }
    try { execute("DETACH DATA ERASURE POLICY ON TABLE " + tblName + " COLUMN " + COLUMN); }
    catch (CommandProcessorException expected) {  }
    try { execute("DROP TABLE IF EXISTS " + tblName); }
    catch (CommandProcessorException expected) {  }
  }
}
