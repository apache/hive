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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hive.ql.optimizer.calcite;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.metadata.StringAppender;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.QueryTables;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.logging.log4j.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class TestRelOptHiveTableLogs {

  private static final String MISSING_STATS_MSG = "No Stats for default@dummy, Columns: col1";

  private StringAppender appender;

  @Before
  public void setUp() {
    appender = StringAppender.createStringAppender("%p %m%n");
    appender.addToLogger(RelOptHiveTable.class.getName(), Level.INFO);
    appender.start();
  }

  @After
  public void tearDown() {
    appender.removeFromLogger(RelOptHiveTable.class.getName());
  }

  @Test
  public void testMissingStatsLoggedAsWarnWhenNotAllowed() {
    AtomicInteger noColsMissingStats = new AtomicInteger(0);
    RelOptHiveTable table = dummyRelOptTable(new JavaTypeFactoryImpl(), noColsMissingStats);

    RuntimeException e = assertThrows(RuntimeException.class,
        () -> table.getColStat(Collections.singletonList(0), false));

    assertEquals(MISSING_STATS_MSG, e.getMessage());
    assertEquals(1, noColsMissingStats.get());
    String output = appender.getOutput();
    assertTrue(output, output.contains("WARN " + MISSING_STATS_MSG));
    assertFalse(output, output.contains("ERROR " + MISSING_STATS_MSG));
  }

  @Test
  public void testMissingStatsLoggedAsWarnWhenAllowed() {
    AtomicInteger noColsMissingStats = new AtomicInteger(0);
    RelOptHiveTable table = dummyRelOptTable(new JavaTypeFactoryImpl(), noColsMissingStats);

    table.getColStat(Collections.singletonList(0), true);

    assertEquals(1, noColsMissingStats.get());
    String output = appender.getOutput();
    assertTrue(output, output.contains("WARN " + MISSING_STATS_MSG));
    assertFalse(output, output.contains("ERROR " + MISSING_STATS_MSG));
  }

  private static RelOptHiveTable dummyRelOptTable(RelDataTypeFactory factory, AtomicInteger noColsMissingStats) {
    RelDataType tblType = factory.builder().add("col1", SqlTypeName.INTEGER).add("col2", SqlTypeName.VARCHAR).build();
    Table tblMeta = new Table("default", "dummy");
    tblMeta.setFields(Arrays.asList(new FieldSchema("col1", "int", null), new FieldSchema("col2", "string", null)));
    tblMeta.setProperty(StatsSetupConst.ROW_COUNT, "10");
    tblMeta.setProperty(StatsSetupConst.RAW_DATA_SIZE, "100");
    tblMeta.setProperty(StatsSetupConst.TOTAL_SIZE, "100");
    HiveConf conf = new HiveConf();
    conf.setBoolVar(HiveConf.ConfVars.HIVE_STATS_FETCH_COLUMN_STATS, false);
    List<ColumnInfo> nonPartCols = Arrays.asList(
        new ColumnInfo("col1", TypeInfoFactory.intTypeInfo, "dummy", false),
        new ColumnInfo("col2", TypeInfoFactory.stringTypeInfo, "dummy", false));
    return new RelOptHiveTable(null, factory, Arrays.asList("default", "dummy"), tblType, tblMeta,
        nonPartCols, Collections.emptyList(), Collections.emptyList(), conf,
        new QueryTables(), new HashMap<>(), new HashMap<>(), noColsMissingStats);
  }
}
