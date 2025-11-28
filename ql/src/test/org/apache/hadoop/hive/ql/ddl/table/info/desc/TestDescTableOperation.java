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

package org.apache.hadoop.hive.ql.ddl.table.info.desc;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.util.stream.Stream;

import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.StatObjectConverter;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.ColStatistics;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.stats.StatsUtils;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

/**
 * Tests for DescTableOperation with minimal mocking to reach line 252
 * where null Range values are handled.
 */
public class TestDescTableOperation {

  @ParameterizedTest(name = "{0}")
  @MethodSource("rangeTestCases")
  public void testGetColumnDataForPartitionKeyColumnDifferentRanges(
      String testName,
      Number minValue,
      Number maxValue,
      String expectedMinValue,
      String expectedMaxValue) throws Exception {

    // minimally possible mocking to pass (DDLOperationContext context, DescTableDesc desc) to the constructor
    try (MockedStatic<SessionState> sessionState = mockStatic(SessionState.class);
         MockedStatic<StatsUtils> statsUtils = mockStatic(StatsUtils.class);
         MockedStatic<StatObjectConverter> statConverter = mockStatic(StatObjectConverter.class)) {
      DDLOperationContext mockContext = mock(DDLOperationContext.class);
      DescTableDesc mockDesc = mock(DescTableDesc.class);
      Hive mockDb = mock(Hive.class);
      Table mockTable = mock(Table.class);
      HiveConf mockConf = new HiveConf();
      SessionState mockSessionState = mock(SessionState.class);

      sessionState.when(SessionState::get).thenReturn(mockSessionState);
      when(mockSessionState.isHiveServerQuery()).thenReturn(false);

      TableName tableName = new TableName("hive", "testdb", "testtable");
      when(mockDesc.getTableName()).thenReturn(tableName);
      when(mockDesc.getPartitionSpec()).thenReturn(null);
      when(mockDesc.getResFile()).thenReturn("/tmp/test-result.txt");
      when(mockContext.getConf()).thenReturn(mockConf);
      when(mockContext.getDb()).thenReturn(mockDb);
      when(mockDb.getTable(eq("testdb"), eq("testtable"), any(), anyBoolean(), anyBoolean(), anyBoolean()))
          .thenReturn(mockTable);
      when(mockDesc.getColumnPath()).thenReturn("testdb.testtable.partition_col");
      when(mockDesc.isFormatted()).thenReturn(true);
      when(mockTable.isPartitioned()).thenReturn(true);
      when(mockTable.isPartitionKey("partition_col")).thenReturn(true);

      FieldSchema partitionCol = new FieldSchema("partition_col", "int", "partition column");
      when(mockTable.getPartColByName("partition_col")).thenReturn(partitionCol);

      // Create ColStatistics with the test's Range values
      ColStatistics colStats = new ColStatistics("partition_col", "int");
      colStats.setRange(minValue, maxValue);
      colStats.setNumNulls(0);
      colStats.setCountDistint(100);
      colStats.setAvgColLen(4);

      statsUtils.when(() -> StatsUtils.checkCanProvidePartitionStats(any())).thenReturn(true);
      statsUtils.when(() -> StatsUtils.getColStatsForPartCol(any(), any(), any())).thenReturn(colStats);

      ArgumentCaptor<Object> minValueArgCaptor = ArgumentCaptor.forClass(Object.class);
      ArgumentCaptor<Object> maxnValueArgCaptor = ArgumentCaptor.forClass(Object.class);
      statConverter.when(() -> StatObjectConverter.fillColumnStatisticsData(
          any(), any(), any(), any(), any(), any(),
          minValueArgCaptor.capture(), maxnValueArgCaptor.capture(),
          any(), any(), any(), any(), any(), any(), any(), any()))
          .thenCallRealMethod();

      DescTableOperation operation = new DescTableOperation(mockContext, mockDesc);

      // Execute - should no longer throw NullPointerException with null values of minValue or maxValue
      assertDoesNotThrow(() -> operation.execute(),
          "Should handle Range with null minValue and maxValue without NPE");

      // Verify the String arguments (6 & 7) passed to fillColumnStatisticsData
      assertEquals(expectedMinValue, minValueArgCaptor.getValue(),
          "declow (arg 6) should be " + (expectedMinValue == null ? "null" : expectedMinValue));
      assertEquals(expectedMaxValue, maxnValueArgCaptor.getValue(),
          "dechigh (arg 7) should be " + (expectedMaxValue == null ? "null" : expectedMaxValue));
    }
  }

  static Stream<Arguments> rangeTestCases() {
    return Stream.of(
      Arguments.of("BothNull", null, null, null, null),
      Arguments.of("MinNull", null, 100, null, "100"),
      Arguments.of("MaxNull", 100, null, "100", null),
      Arguments.of("NeitherNull", 100, 200, "100", "200")
    );
  }

}
