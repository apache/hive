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

package org.apache.hadoop.hive.metastore.tools.schematool;

import org.apache.hadoop.hive.metastore.HiveMetaException;
import org.apache.hadoop.hive.metastore.IMetaStoreSchemaInfo;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Category(MetastoreUnitTest.class)
public class TestSchemaToolTaskRebuildIndexes {

  @Rule
  public TemporaryFolder tmp = new TemporaryFolder();

  private MetastoreSchemaTool schemaTool;
  private IMetaStoreSchemaInfo schemaInfo;
  private SchemaToolTaskRebuildIndexes task;

  @Before
  public void setUp() {
    schemaTool = mock(MetastoreSchemaTool.class);
    schemaInfo = mock(IMetaStoreSchemaInfo.class);
    task = new SchemaToolTaskRebuildIndexes();
    task.schemaTool = schemaTool;
  }

  @Test
  public void oracleMissingDropIndexIsIgnoredAndCreateStillRuns() throws Exception {
    File scriptDir = tmp.newFolder("scripts");
    File script = new File(scriptDir, SchemaToolTaskRebuildIndexes.REBUILD_INDEXES_FILE_PREFIX + ".oracle.sql");
    Files.writeString(script.toPath(),
        "DROP INDEX MISSING_IDX;\nCREATE INDEX MISSING_IDX ON TBLS(TBL_ID);\n",
        StandardCharsets.UTF_8);

    Connection connection = mock(Connection.class);
    Statement statement = mock(Statement.class);
    // Ignore missing index on DROP.
    SQLException missingIndex = new SQLException("index does not exist", "42000", 1418);
    when(statement.execute("DROP INDEX MISSING_IDX")).thenThrow(missingIndex);
    when(statement.execute("CREATE INDEX MISSING_IDX ON TBLS(TBL_ID)")).thenReturn(false);
    when(connection.createStatement()).thenReturn(statement);

    when(schemaTool.getDbType()).thenReturn(HiveSchemaHelper.DB_ORACLE);
    when(schemaTool.getMetaStoreSchemaInfo()).thenReturn(schemaInfo);
    when(schemaInfo.getMetaStoreScriptDir()).thenReturn(scriptDir.getAbsolutePath());
    when(schemaTool.getConnectionToMetastore(false)).thenReturn(connection);

    task.execute();

    // CREATE still executes.
    verify(statement).execute("CREATE INDEX MISSING_IDX ON TBLS(TBL_ID)");
    verify(schemaTool, never()).execSql(anyString(), anyString());
  }

  @Test
  public void oracleUnexpectedDropErrorFailsRebuild() throws Exception {
    File scriptDir = tmp.newFolder("scripts");
    File script = new File(scriptDir, SchemaToolTaskRebuildIndexes.REBUILD_INDEXES_FILE_PREFIX + ".oracle.sql");
    Files.writeString(script.toPath(), "DROP INDEX BAD_IDX;\n", StandardCharsets.UTF_8);

    Connection connection = mock(Connection.class);
    Statement statement = mock(Statement.class);
    // Non-1418 errors must fail.
    when(statement.execute("DROP INDEX BAD_IDX")).thenThrow(new SQLException("boom", "42000", 942));
    when(connection.createStatement()).thenReturn(statement);

    when(schemaTool.getDbType()).thenReturn(HiveSchemaHelper.DB_ORACLE);
    when(schemaTool.getMetaStoreSchemaInfo()).thenReturn(schemaInfo);
    when(schemaInfo.getMetaStoreScriptDir()).thenReturn(scriptDir.getAbsolutePath());
    when(schemaTool.getConnectionToMetastore(false)).thenReturn(connection);

    try {
      task.execute();
      fail("Expected HiveMetaException");
    } catch (HiveMetaException e) {
      assertTrue(e.getMessage().contains("Index rebuild failed"));
    }
  }

  @Test
  public void oracleUnterminatedStatementFailsFast() throws Exception {
    File scriptDir = tmp.newFolder("scripts");
    File script = new File(scriptDir, SchemaToolTaskRebuildIndexes.REBUILD_INDEXES_FILE_PREFIX + ".oracle.sql");
    // Missing semicolon should fail before execution.
    Files.writeString(script.toPath(), "DROP INDEX INCOMPLETE", StandardCharsets.UTF_8);

    Connection connection = mock(Connection.class);
    Statement statement = mock(Statement.class);
    when(connection.createStatement()).thenReturn(statement);

    when(schemaTool.getDbType()).thenReturn(HiveSchemaHelper.DB_ORACLE);
    when(schemaTool.getMetaStoreSchemaInfo()).thenReturn(schemaInfo);
    when(schemaInfo.getMetaStoreScriptDir()).thenReturn(scriptDir.getAbsolutePath());
    when(schemaTool.getConnectionToMetastore(false)).thenReturn(connection);

    try {
      task.execute();
      fail("Expected HiveMetaException");
    } catch (HiveMetaException e) {
      assertTrue(e.getMessage().contains("Oracle rebuild-indexes script contains an unterminated SQL statement."));
    }

    verify(statement, times(0)).execute(anyString());
  }

  @Test
  public void nonOracleUsesExistingExecSqlPath() throws Exception {
    File scriptDir = tmp.newFolder("scripts");
    File script = new File(scriptDir, SchemaToolTaskRebuildIndexes.REBUILD_INDEXES_FILE_PREFIX + ".postgres.sql");
    Files.writeString(script.toPath(), "DROP INDEX IF EXISTS X;\n", StandardCharsets.UTF_8);

    when(schemaTool.getDbType()).thenReturn(HiveSchemaHelper.DB_POSTGRES);
    when(schemaTool.getMetaStoreSchemaInfo()).thenReturn(schemaInfo);
    when(schemaInfo.getMetaStoreScriptDir()).thenReturn(scriptDir.getAbsolutePath());

    task.execute();

    // Non-Oracle path stays on execSql.
    verify(schemaTool).execSql(scriptDir.getAbsolutePath(),
        SchemaToolTaskRebuildIndexes.REBUILD_INDEXES_FILE_PREFIX + ".postgres.sql");
    verify(schemaTool, never()).getConnectionToMetastore(false);
  }
}
