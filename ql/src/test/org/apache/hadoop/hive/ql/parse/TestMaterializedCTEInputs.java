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

package org.apache.hadoop.hive.ql.parse;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConfForTest;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.database.drop.DropDatabaseDesc;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestMaterializedCTEInputs {
  private static final String DB_NAME = "test_materialized_cte";
  private static final String TABLE_FQ_NAME = DB_NAME + "@src";

  private static Hive db;
  private static HiveConf conf;

  @BeforeAll
  public static void beforeClass() throws Exception {
    conf = new HiveConfForTest(TestMaterializedCTEInputs.class);
    conf.set("hive.security.authorization.enabled", "false");
    conf.set("hive.security.authorization.manager",
        "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdConfOnlyAuthorizerFactory");
    conf.setIntVar(HiveConf.ConfVars.HIVE_CTE_MATERIALIZE_THRESHOLD, 1);
    conf.setBoolVar(HiveConf.ConfVars.HIVE_CTE_MATERIALIZE_FULL_AGGREGATE_ONLY, false);
    conf.setBoolVar(HiveConf.ConfVars.HIVE_STATS_COLLECT_SCANCOLS, true);
    db = Hive.get(conf);
    SessionState.start(conf);
    new DatabaseBuilder().setName(DB_NAME).create(db.getMSC(), conf);
    SessionState.get().setCurrentDatabase(DB_NAME);
    new TableBuilder().setDbName(DB_NAME).setTableName("src")
        .addCol("key", "string")
        .addCol("value", "string")
        .addCol("col1", "int")
        .create(db.getMSC(), conf);
  }

  public static Stream<Arguments> casesForMaterializedCteInputs() {
    return Stream.of(
        Arguments.of("chain cte", "with q1 as ( select key from q2 where key = '5'),"
            + "q2 as ( select key from test_materialized_cte.src where key = '5') "
            + "select * from (select key from q1) a", Set.of("key")),
        Arguments.of("nested cte", "WITH q1 AS ("
            + "WITH q2 AS (SELECT key, value FROM test_materialized_cte.src WHERE key = '4') "
            + "SELECT * FROM q2 UNION ALL SELECT * FROM q2) "
            + "SELECT * FROM q1 t1 JOIN q1 t2 ON t1.key = t2.key", Set.of("key", "value")),
        Arguments.of("merge columns", "WITH q1 AS ("
                + "WITH q2 AS (SELECT key FROM test_materialized_cte.src WHERE key = '4') "
                + "SELECT * FROM q2 UNION ALL SELECT * FROM q2) "
                + "SELECT * FROM q1 t1 JOIN test_materialized_cte.src t2 ON t1.key = t2.key", Set.of("key", "value"))
    );
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("casesForMaterializedCteInputs")
  void testMaterializedCteInputs(String type, String query, Set<String> expectedCols) throws Exception {
    HiveConf testConf = new HiveConf(conf);
    Context ctx = new Context(testConf);
    ASTNode astNode = ParseUtils.parse(query, ctx);
    QueryState queryState = new QueryState.Builder().withHiveConf(testConf).build();
    SemanticAnalyzer analyzer = (SemanticAnalyzer) SemanticAnalyzerFactory.get(queryState, astNode);
    analyzer.initCtx(ctx);
    analyzer.analyze(astNode, ctx);

    Set<ReadEntity> allInputs = analyzer.getAllInputs();

    assertTrue(allInputs.stream().anyMatch(e -> isTableNamed(e, "src")),
        "Materialized CTE base table must appear in getAllInputs");

    ColumnAccessInfo columnAccessInfo = analyzer.getColumnAccessInfo();
    assertNotNull(columnAccessInfo);
    List<String> srcCols = columnAccessInfo.getTableToColumnAccessMap().get(TABLE_FQ_NAME);
    assertNotNull(srcCols, "Column must include materialized CTE base table");
    assertTrue(new HashSet<>(srcCols).containsAll(expectedCols),
        () -> "Expected columns " + expectedCols + " but got " + srcCols);
    ctx.clear();
  }

  private static boolean isTableNamed(ReadEntity entity, String tableName) {
    return entity.getTable() != null && tableName.equals(entity.getTable().getTableName());
  }

  @AfterAll
  public static void afterClass() throws Exception {
    try {
      db.dropDatabase(new DropDatabaseDesc(DB_NAME, DB_NAME, true, true, true));
    } catch (Exception ignored) {
    }
    db.close(true);
  }
}
