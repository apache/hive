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

package org.apache.hadoop.hive.ql.anon.e2e;

import org.apache.hadoop.hive.metastore.api.ColumnInternalFormat;
import org.apache.hadoop.hive.ql.anon.BaseTest;
import org.apache.hadoop.hive.ql.anon.FileType;
import org.apache.hadoop.hive.ql.anon.TestUtils;
import org.apache.hadoop.hive.ql.anon.builders.*;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class BaseEndToEndTest extends BaseTest {

  protected String policyName = "policy1_" + Long.toString(System.nanoTime(), 36);
  protected ColumnInternalFormat internalFormat;
  protected FileType fileType;
  protected String indexName;
  protected int userId = 1;
  protected String mColName = "m";
  protected String oColName = "o";
  protected String bColName = "b";
  protected int fieldLength = 20;

  private boolean policyProvisioned = false;

  private static final String POLICY_DSL =
      """
      VERSION v1
      IDENTITY userId TYPE INT
      SCHEMA   TYPE INT
      FOR SCHEMA 3
          ERASE country, city, telephone, ipList
      """;

  @Override
  protected String[] managedErasurePolicies() {
    return new String[] { policyName };
  }

  protected void create() throws CommandProcessorException {
    try { execute("DETACH DATA ERASURE POLICY ON TABLE " + tblName + " COLUMN " + bColName); }
    catch (CommandProcessorException ignore) {  }
    for (final String suffix : new String[] {"bt", "lk", "tb"}) {
      try {
        execute(new DropIndexStatementBuilder(getIndexName(suffix), tblName).withIfExists().build());
      } catch (CommandProcessorException ignore) {
      }
    }
    try { execute("drop table if exists " + tblName); }
    catch (CommandProcessorException ignore) {  }
    final String cmd = new CreateTableStatementBuilder(tblName, mColName, oColName, bColName)
      .withInternalFormat(internalFormat)
      .withFileType(fileType)
      .withIfNotExists()
      .build();
    final List<Object> lst = execute(cmd);
    Assertions.assertEquals(0, lst.size());
  }

  protected void insert() throws CommandProcessorException {
    final List<String> commands = TestUtils.getInsertCommands(tblName, userId, fieldLength, internalFormat);
    for (final String cmd : commands) {
      final List<Object> lst = execute(cmd);
      Assertions.assertEquals(0, lst.size());
    }
  }

  protected void createPolicy() throws CommandProcessorException {
    if (policyProvisioned) {
      return;
    }
    try {
      final Path policyFile = Files.createTempFile(policyName + "_", ".erp");
      Files.write(policyFile, POLICY_DSL.getBytes());
      execute("LOAD ERASURE POLICY " + policyName + " FROM '" + policyFile + "'");
      execute("VALIDATE ERASURE POLICY " + policyName);
      execute("ACTIVATE ERASURE POLICY " + policyName);
    } catch (IOException ioe) {
      throw new CommandProcessorException(ioe);
    }
    policyProvisioned = true;
  }

  protected void setPolicy() throws CommandProcessorException {
    final String cmd = ("ATTACH DATA ERASURE POLICY %s ON TABLE %s COLUMN %s"
        + " WITH ( SCHEMA FIELD (%s), ROW LOCATOR (%s),"
        + " COLUMN FORMAT (%s) )"
        + " RESOLUTION ( EXPLICIT )").formatted(policyName, tblName, bColName, mColName, oColName, internalFormat.name());
    final List<Object> lst = execute(cmd);
    Assertions.assertEquals(0, lst.size());
  }

  protected void anonymizeTable() throws CommandProcessorException {
    final String cmd = new EraseStatementBuilder(tblName).withIds(userId).build();
    final List<Object> lst = execute(cmd);
    Assertions.assertEquals(0, lst.size());
  }

  protected void selectTable() throws CommandProcessorException {
    final String cmd = new SelectStatementBuilder(tblName, mColName, oColName, bColName, internalFormat).build();
    final List<Object> lst = execute(cmd);
    Assertions.assertEquals(7, lst.size());
    TestUtils.validateRowSet(lst, internalFormat, userId);
  }

  protected void unsetPolicy() throws CommandProcessorException {
    try { execute("DETACH DATA ERASURE POLICY ON TABLE " + tblName + " COLUMN " + bColName); }
    catch (CommandProcessorException ignore) {  }
  }

  protected void dropTable() throws CommandProcessorException {
    final String cmd = new DropTableStatementBuilder(tblName).withIfExists().build();
    try { execute(cmd); }
    catch (CommandProcessorException ignore) {  }
  }

  protected void createBtreeIndex() throws CommandProcessorException {
    final String sql = new CreateIndexStatementBuilder(indexName, tblName, bColName)
      .withBtreeOptions(8192, 10, "int")
      .withIfNotExists()
      .build();
    List<Object> lst = execute(sql);
    Assertions.assertEquals(0, lst.size());
  }

  protected void createDirectoryIndex() throws CommandProcessorException {
    final String cmd = new CreateIndexStatementBuilder(indexName, tblName, bColName)
      .withDirectoryOptions("int").withIfNotExists()
      .build();
    final List<Object> lst = execute(cmd);
    Assertions.assertEquals(0, lst.size());
  }

  protected void createTabularIndex() throws CommandProcessorException {
    final String cmd = new CreateIndexStatementBuilder(indexName, tblName, bColName)
      .withTabularOptions().withIfNotExists().build();
    final List<Object> lst = execute(cmd);
    Assertions.assertEquals(0, lst.size());
  }

  protected void alterIndex() throws CommandProcessorException {
    final String cmd = new AlterIndexStatementBuilder(indexName, tblName).build();
    final List<Object> lst = execute(cmd);
    Assertions.assertEquals(0, lst.size());
  }

  protected void dropIndex() throws CommandProcessorException {
    final String cmd = new DropIndexStatementBuilder(indexName, tblName).withIfExists().build();
    final List<Object> lst = execute(cmd);
    Assertions.assertEquals(0, lst.size());
  }

  private String runId;

  public void testNoIx(final ColumnInternalFormat internalFormat, final FileType fileType) throws CommandProcessorException {
    this.internalFormat = internalFormat;
    this.fileType = fileType;
    this.runId = Long.toString(System.nanoTime(), 36);
    this.tblName = getTableName();
    create();
    truncate();
    insert();
    createPolicy();
    setPolicy();
    anonymizeTable();
    selectTable();
    unsetPolicy();
    dropTable();
  }

  public void testBtree(final ColumnInternalFormat internalFormat, final FileType fileType) throws CommandProcessorException {
    this.internalFormat = internalFormat;
    this.fileType = fileType;
    this.runId = Long.toString(System.nanoTime(), 36);
    this.tblName = getTableName();
    this.indexName = getIndexName("bt");
    create();
    truncate();
    insert();
    createPolicy();
    setPolicy();
    createBtreeIndex();
    alterIndex();
    anonymizeTable();
    selectTable();
    unsetPolicy();
    dropIndex();
    dropTable();
  }

  public void testDirectory(final ColumnInternalFormat internalFormat, final FileType fileType) throws CommandProcessorException {
    this.internalFormat = internalFormat;
    this.fileType = fileType;
    this.runId = Long.toString(System.nanoTime(), 36);
    this.tblName = getTableName();
    this.indexName = getIndexName("lk");
    create();
    truncate();
    insert();
    createPolicy();
    setPolicy();
    createDirectoryIndex();
    alterIndex();
    anonymizeTable();
    selectTable();
    unsetPolicy();
    dropIndex();
    dropTable();
  }

  public void testTabular(final ColumnInternalFormat internalFormat, final FileType fileType) throws CommandProcessorException {
    this.internalFormat = internalFormat;
    this.fileType = fileType;
    this.runId = Long.toString(System.nanoTime(), 36);
    this.tblName = getTableName();
    this.indexName = getIndexName("tb");
    create();
    truncate();
    insert();
    createPolicy();
    setPolicy();
    createTabularIndex();
    alterIndex();
    anonymizeTable();
    selectTable();
    unsetPolicy();
    dropIndex();
    dropTable();
  }

  private String getTableName() {
    final String type = this.fileType.name().toLowerCase().substring(0, 1);
    final String format = internalFormat.name().toLowerCase().substring(0, 1);
    return "t_" + type + "_" + format + "_" + runId;
  }

  private String getIndexName(final String suffix) {
    final String format = internalFormat.name().toLowerCase();
    return "ix_" + format + "_" + suffix + "_" + runId;
  }
}
