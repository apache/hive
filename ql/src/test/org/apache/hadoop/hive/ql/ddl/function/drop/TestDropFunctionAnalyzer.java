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

package org.apache.hadoop.hive.ql.ddl.function.drop;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.conf.HiveConfForTest;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.FunctionType;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for DropFunctionAnalyzer focusing on the case where the function's JAR resource is
 * unavailable (e.g. deleted from HDFS after the function was registered).
 */
class TestDropFunctionAnalyzer {

  private HiveConf conf;

  @BeforeEach
  void setUp() {
    conf = new HiveConfForTest(getClass());
    SessionState.start(conf);
  }

  @AfterEach
  void tearDown() throws Exception {
    SessionState ss = SessionState.get();
    if (ss != null) {
      ss.close();
    }
  }

  /** Simulates JAR-unavailable by returning null from getFunctionInfo. */
  private DropFunctionAnalyzer analyzerWithUnavailableJar(Hive mockDb) throws SemanticException {
    QueryState queryState = QueryState.getNewQueryState(conf, null);
    queryState.setCommandType(HiveOperation.DROPFUNCTION);
    return new DropFunctionAnalyzer(queryState, mockDb) {
      @Override
      protected FunctionInfo getFunctionInfo(String functionName) throws SemanticException {
        return null;
      }
    };
  }

  /**
   * When getFunctionInfo returns null (JAR unavailable) but the function still exists in the
   * metastore, DROP FUNCTION must proceed and emit a drop task so the orphaned definition is
   * actually removed.
   */
  @Test
  void testDropSucceedsWhenJarUnavailableButFunctionInMetastore() throws Exception {
    Hive mockDb = mock(Hive.class);
    Function msFunction = new Function("dummy", "default", "com.example.DummyUDF",
        "user", PrincipalType.USER, 0, FunctionType.JAVA, Collections.emptyList());
    Database msDatabase = new Database("default", "", "/tmp", Collections.emptyMap());

    when(mockDb.getFunction("default", "dummy")).thenReturn(msFunction);
    when(mockDb.getDatabase("default")).thenReturn(msDatabase);
    when(mockDb.getDatabase(any(), eq("default"))).thenReturn(msDatabase);

    DropFunctionAnalyzer analyzer = analyzerWithUnavailableJar(mockDb);
    analyzer.analyzeInternal(parse("drop function dummy"));

    assertEquals(1, analyzer.getRootTasks().size(), "Expected one DROP task even when JAR is unavailable");
  }

  /**
   * When the function does not exist in either the session registry or the metastore, DROP FUNCTION
   * (without IF EXISTS) must surface the error to the client.
   */
  @Test
  void testDropThrowsWhenFunctionNotInMetastore() throws Exception {
    conf.setBoolVar(ConfVars.DROP_IGNORES_NON_EXISTENT, false);
    Hive mockDb = mock(Hive.class);
    when(mockDb.getFunction(anyString(), anyString())).thenThrow(new HiveException("not found"));

    DropFunctionAnalyzer analyzer = analyzerWithUnavailableJar(mockDb);
    assertThrows(SemanticException.class, () -> analyzer.analyzeInternal(parse("drop function dummy")),
        "Expected SemanticException when function not in registry or metastore");
  }

  /**
   * DROP FUNCTION IF EXISTS must silently succeed (no task, no exception) when the function is
   * absent from both the session registry and the metastore.
   */
  @Test
  void testDropIfExistsSilentWhenFunctionAbsent() throws Exception {
    Hive mockDb = mock(Hive.class);
    when(mockDb.getFunction(anyString(), anyString())).thenThrow(new HiveException("not found"));

    DropFunctionAnalyzer analyzer = analyzerWithUnavailableJar(mockDb);
    analyzer.analyzeInternal(parse("drop function if exists dummy"));

    assertEquals(0, analyzer.getRootTasks().size(), "Expected no tasks when function is absent and IF EXISTS is set");
  }

  private ASTNode parse(String sql) throws Exception {
    return ParseUtils.parse(sql, new Context(conf));
  }
}
