/*
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.storage.jdbc.dao;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hive.storage.jdbc.DBRecordWritable;
import org.apache.hive.storage.jdbc.conf.JdbcStorageConfig;
import org.apache.hive.storage.jdbc.exception.HiveJdbcDatabaseAccessException;

import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.equalToIgnoringCase;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestGenericJdbcDatabaseAccessor {

  private static final String BOUNDARY_SQL = "select * from test_strategy";

  @Test
  public void testGetColumnNames_starQuery() throws HiveJdbcDatabaseAccessException {
    Configuration conf = buildConfiguration();
    DatabaseAccessor accessor = DatabaseAccessorFactory.getAccessor(conf);
    List<String> columnNames = accessor.getColumnNames(conf);

    assertThat(columnNames, is(notNullValue()));
    assertThat(columnNames.size(), is(equalTo(7)));
    assertThat(columnNames.get(0), is(equalToIgnoringCase("strategy_id")));
  }

  @Test
  public void testGetColumnTypes_starQuery_allTypes() throws HiveJdbcDatabaseAccessException {
    Configuration conf = buildConfiguration();
    conf.set(JdbcStorageConfig.QUERY.getPropertyName(), "select * from all_types_table");
    DatabaseAccessor accessor = DatabaseAccessorFactory.getAccessor(conf);

    List<TypeInfo> expectedTypes = new ArrayList<>();
    expectedTypes.add(TypeInfoFactory.getCharTypeInfo(1));
    expectedTypes.add(TypeInfoFactory.getCharTypeInfo(20));
    expectedTypes.add(TypeInfoFactory.unknownTypeInfo);
    expectedTypes.add(TypeInfoFactory.varcharTypeInfo);
    expectedTypes.add(TypeInfoFactory.getVarcharTypeInfo(1024));
    expectedTypes.add(TypeInfoFactory.varcharTypeInfo);
    expectedTypes.add(TypeInfoFactory.booleanTypeInfo);
    expectedTypes.add(TypeInfoFactory.byteTypeInfo);
    expectedTypes.add(TypeInfoFactory.shortTypeInfo);
    expectedTypes.add(TypeInfoFactory.intTypeInfo);
    expectedTypes.add(TypeInfoFactory.longTypeInfo);
    expectedTypes.add(TypeInfoFactory.getDecimalTypeInfo(38, 0));
    expectedTypes.add(TypeInfoFactory.getDecimalTypeInfo(9, 3));
    expectedTypes.add(TypeInfoFactory.floatTypeInfo);
    expectedTypes.add(TypeInfoFactory.doubleTypeInfo);
    expectedTypes.add(TypeInfoFactory.getDecimalTypeInfo(38, 0));
    expectedTypes.add(TypeInfoFactory.dateTypeInfo);
    expectedTypes.add(TypeInfoFactory.unknownTypeInfo);
    expectedTypes.add(TypeInfoFactory.unknownTypeInfo);
    expectedTypes.add(TypeInfoFactory.timestampTypeInfo);
    expectedTypes.add(TypeInfoFactory.timestampTypeInfo);
    expectedTypes.add(TypeInfoFactory.unknownTypeInfo);
    expectedTypes.add(TypeInfoFactory.unknownTypeInfo);
    expectedTypes.add(TypeInfoFactory.unknownTypeInfo);
    expectedTypes.add(TypeInfoFactory.unknownTypeInfo);
    expectedTypes.add(TypeInfoFactory.unknownTypeInfo);
    expectedTypes.add(TypeInfoFactory.unknownTypeInfo);
    expectedTypes.add(TypeInfoFactory.unknownTypeInfo);
    expectedTypes.add(TypeInfoFactory.unknownTypeInfo);
    expectedTypes.add(TypeInfoFactory.unknownTypeInfo);
    expectedTypes.add(TypeInfoFactory.getListTypeInfo(TypeInfoFactory.unknownTypeInfo));
    expectedTypes.add(TypeInfoFactory.unknownTypeInfo);
    Assert.assertEquals(expectedTypes, accessor.getColumnTypes(conf));
  }

  @Test
  public void testGetColumnNames_fieldListQuery() throws HiveJdbcDatabaseAccessException {
    Configuration conf = buildConfiguration();
    conf.set(JdbcStorageConfig.QUERY.getPropertyName(), "select name,referrer from test_strategy");
    DatabaseAccessor accessor = DatabaseAccessorFactory.getAccessor(conf);
    List<String> columnNames = accessor.getColumnNames(conf);

    assertThat(columnNames, is(notNullValue()));
    assertThat(columnNames.size(), is(equalTo(2)));
    assertThat(columnNames.get(0), is(equalToIgnoringCase("name")));
  }

  @Test
  public void testGetColumnTypes_fieldListQuery() throws HiveJdbcDatabaseAccessException {
    Configuration conf = buildConfiguration();
    conf.set(JdbcStorageConfig.QUERY.getPropertyName(), "select name,referrer from test_strategy");
    DatabaseAccessor accessor = DatabaseAccessorFactory.getAccessor(conf);

    List<TypeInfo> expectedTypes = new ArrayList<>(2);
    expectedTypes.add(TypeInfoFactory.getVarcharTypeInfo(50));
    expectedTypes.add(TypeInfoFactory.getVarcharTypeInfo(1024));
    Assert.assertEquals(expectedTypes, accessor.getColumnTypes(conf));
  }


  @Test(expected = HiveJdbcDatabaseAccessException.class)
  public void testGetColumnNames_invalidQuery() throws HiveJdbcDatabaseAccessException {
    Configuration conf = buildConfiguration();
    conf.set(JdbcStorageConfig.QUERY.getPropertyName(), "select * from invalid_strategy");
    DatabaseAccessor accessor = DatabaseAccessorFactory.getAccessor(conf);
    @SuppressWarnings("unused")
      List<String> columnNames = accessor.getColumnNames(conf);
  }


  @Test
  public void testGetTotalNumberOfRecords() throws HiveJdbcDatabaseAccessException {
    Configuration conf = buildConfiguration();
    DatabaseAccessor accessor = DatabaseAccessorFactory.getAccessor(conf);
    int numRecords = accessor.getTotalNumberOfRecords(conf);

    assertThat(numRecords, is(equalTo(5)));
  }


  @Test
  public void testGetTotalNumberOfRecords_whereClause() throws HiveJdbcDatabaseAccessException {
    Configuration conf = buildConfiguration();
    conf.set(JdbcStorageConfig.QUERY.getPropertyName(), "select * from test_strategy where strategy_id = '5'");
    DatabaseAccessor accessor = DatabaseAccessorFactory.getAccessor(conf);
    int numRecords = accessor.getTotalNumberOfRecords(conf);

    assertThat(numRecords, is(equalTo(1)));
  }


  @Test
  public void testGetTotalNumberOfRecords_noRecords() throws HiveJdbcDatabaseAccessException {
    Configuration conf = buildConfiguration();
    conf.set(JdbcStorageConfig.QUERY.getPropertyName(), "select * from test_strategy where strategy_id = '25'");
    DatabaseAccessor accessor = DatabaseAccessorFactory.getAccessor(conf);
    int numRecords = accessor.getTotalNumberOfRecords(conf);

    assertThat(numRecords, is(equalTo(0)));
  }


  @Test(expected = HiveJdbcDatabaseAccessException.class)
  public void testGetTotalNumberOfRecords_invalidQuery() throws HiveJdbcDatabaseAccessException {
    Configuration conf = buildConfiguration();
    conf.set(JdbcStorageConfig.QUERY.getPropertyName(), "select * from strategyx where strategy_id = '5'");
    DatabaseAccessor accessor = DatabaseAccessorFactory.getAccessor(conf);
    @SuppressWarnings("unused")
      int numRecords = accessor.getTotalNumberOfRecords(conf);
  }


  @Test
  public void testGetRecordIterator() throws HiveJdbcDatabaseAccessException {
    Configuration conf = buildConfiguration();
    DatabaseAccessor accessor = DatabaseAccessorFactory.getAccessor(conf);
    JdbcRecordIterator iterator = accessor.getRecordIterator(conf, null, null, null,2, 0);

    assertThat(iterator, is(notNullValue()));

    int count = 0;
    while (iterator.hasNext()) {
      Map<String, Object> record = iterator.next();
      count++;

      assertThat(record, is(notNullValue()));
      assertThat(record.size(), is(equalTo(7)));
      assertThat(record.get("strategy_id"), is(equalTo(count)));
    }

    assertThat(count, is(equalTo(2)));
    iterator.close();
  }


  @Test
  public void testGetRecordIterator_offsets() throws HiveJdbcDatabaseAccessException {
    Configuration conf = buildConfiguration();
    DatabaseAccessor accessor = DatabaseAccessorFactory.getAccessor(conf);
    JdbcRecordIterator iterator = accessor.getRecordIterator(conf, null, null, null, 2, 2);

    assertThat(iterator, is(notNullValue()));

    int count = 0;
    while (iterator.hasNext()) {
      Map<String, Object> record = iterator.next();
      count++;

      assertThat(record, is(notNullValue()));
      assertThat(record.size(), is(equalTo(7)));
      assertThat(record.get("strategy_id"), is(equalTo(count + 2)));
    }

    assertThat(count, is(equalTo(2)));
    iterator.close();
  }


  @Test
  public void testGetRecordIterator_limitOffsetLastPartition() throws HiveJdbcDatabaseAccessException {
    Configuration conf = buildConfiguration();
    DatabaseAccessor accessor = DatabaseAccessorFactory.getAccessor(conf);
    JdbcRecordIterator iterator = accessor.getRecordIterator(conf, null, null, null, 2, 4);

    assertThat(iterator, is(notNullValue()));

    int count = 0;
    while (iterator.hasNext()) {
      Map<String, Object> record = iterator.next();
      count++;

      assertThat(record, is(notNullValue()));
      assertThat(record.size(), is(equalTo(7)));
      assertThat(record.get("strategy_id"), is(equalTo(5)));
    }

    assertThat(count, is(equalTo(1)));
    iterator.close();
  }


  @Test
  public void testGetRecordIterator_emptyResultSet() throws HiveJdbcDatabaseAccessException {
    Configuration conf = buildConfiguration();
    conf.set(JdbcStorageConfig.QUERY.getPropertyName(), "select * from test_strategy where strategy_id = '25'");
    DatabaseAccessor accessor = DatabaseAccessorFactory.getAccessor(conf);
    JdbcRecordIterator iterator = accessor.getRecordIterator(conf, null, null, null, 0, 2);

    assertThat(iterator, is(notNullValue()));
    assertThat(iterator.hasNext(), is(false));
    iterator.close();
  }


  @Test
  public void testGetRecordIterator_partitionFirst() throws HiveJdbcDatabaseAccessException {
    Configuration conf = buildConfiguration();
    DatabaseAccessor accessor = DatabaseAccessorFactory.getAccessor(conf);
    JdbcRecordIterator iterator =
        accessor.getRecordIterator(conf, getStrategyIdColumnName(conf, accessor), null, "3", -1, 0);

    assertThat(iterator, is(notNullValue()));

    int count = 0;
    while (iterator.hasNext()) {
      Map<String, Object> record = iterator.next();
      count++;

      assertThat(record, is(notNullValue()));
      assertThat(record.size(), is(equalTo(7)));
      assertThat(record.get("strategy_id"), is(equalTo(count)));
    }

    assertThat(count, is(equalTo(2)));
    iterator.close();
  }


  @Test
  public void testGetRecordIterator_partitionMiddle() throws HiveJdbcDatabaseAccessException {
    Configuration conf = buildConfiguration();
    DatabaseAccessor accessor = DatabaseAccessorFactory.getAccessor(conf);
    JdbcRecordIterator iterator =
        accessor.getRecordIterator(conf, getStrategyIdColumnName(conf, accessor), "3", "5", -1, 0);

    assertThat(iterator, is(notNullValue()));

    int count = 0;
    while (iterator.hasNext()) {
      Map<String, Object> record = iterator.next();
      count++;

      assertThat(record, is(notNullValue()));
      assertThat(record.size(), is(equalTo(7)));
      assertThat(record.get("strategy_id"), is(equalTo(count + 2)));
    }

    assertThat(count, is(equalTo(2)));
    iterator.close();
  }


  @Test
  public void testGetRecordIterator_partitionLast() throws HiveJdbcDatabaseAccessException {
    Configuration conf = buildConfiguration();
    DatabaseAccessor accessor = DatabaseAccessorFactory.getAccessor(conf);
    JdbcRecordIterator iterator =
        accessor.getRecordIterator(conf, getStrategyIdColumnName(conf, accessor), "5", null, -1, 0);

    assertThat(iterator, is(notNullValue()));

    int count = 0;
    while (iterator.hasNext()) {
      Map<String, Object> record = iterator.next();
      count++;

      assertThat(record, is(notNullValue()));
      assertThat(record.size(), is(equalTo(7)));
      assertThat(record.get("strategy_id"), is(equalTo(5)));
    }

    assertThat(count, is(equalTo(1)));
    iterator.close();
  }


  @Test
  public void testGetRecordIterator_partitionEmptyResultSet() throws HiveJdbcDatabaseAccessException {
    Configuration conf = buildConfiguration();
    DatabaseAccessor accessor = DatabaseAccessorFactory.getAccessor(conf);
    JdbcRecordIterator iterator =
        accessor.getRecordIterator(conf, getStrategyIdColumnName(conf, accessor), "6", "7", -1, 0);

    assertThat(iterator, is(notNullValue()));
    assertThat(iterator.hasNext(), is(false));
    iterator.close();
  }


  @Test
  public void testGetRecordIterator_largeOffset() throws HiveJdbcDatabaseAccessException {
    Configuration conf = buildConfiguration();
    DatabaseAccessor accessor = DatabaseAccessorFactory.getAccessor(conf);
    JdbcRecordIterator iterator = accessor.getRecordIterator(conf, null, null, null, 10, 25);

    assertThat(iterator, is(notNullValue()));
    assertThat(iterator.hasNext(), is(false));
    iterator.close();
  }


  @Test(expected = HiveJdbcDatabaseAccessException.class)
  public void testGetRecordIterator_invalidQuery() throws HiveJdbcDatabaseAccessException {
    Configuration conf = buildConfiguration();
    conf.set(JdbcStorageConfig.QUERY.getPropertyName(), "select * from strategyx");
    DatabaseAccessor accessor = DatabaseAccessorFactory.getAccessor(conf);
    @SuppressWarnings("unused")
      JdbcRecordIterator iterator = accessor.getRecordIterator(conf, null, null, null, 0, 2);
  }

  @Test
  public void testGetBounds_minAndMax() throws HiveJdbcDatabaseAccessException {
    Configuration conf = buildConfiguration();
    DatabaseAccessor accessor = DatabaseAccessorFactory.getAccessor(conf);
    Pair<String, String> bounds = accessor.getBounds(conf, getStrategyIdColumnName(conf, accessor), true, true);

    assertThat(bounds.getLeft(), is(equalTo("1")));
    assertThat(bounds.getRight(), is(equalTo("5")));
  }

  @Test
  public void testGetBounds_minOnly() throws HiveJdbcDatabaseAccessException {
    Configuration conf = buildConfiguration();
    DatabaseAccessor accessor = DatabaseAccessorFactory.getAccessor(conf);
    Pair<String, String> bounds = accessor.getBounds(conf, getStrategyIdColumnName(conf, accessor), true, false);

    assertThat(bounds.getLeft(), is(equalTo("1")));
    assertThat(bounds.getRight(), is(nullValue()));
  }

  @Test
  public void testGetBounds_maxOnly() throws HiveJdbcDatabaseAccessException {
    Configuration conf = buildConfiguration();
    DatabaseAccessor accessor = DatabaseAccessorFactory.getAccessor(conf);
    Pair<String, String> bounds = accessor.getBounds(conf, getStrategyIdColumnName(conf, accessor), false, true);

    assertThat(bounds.getLeft(), is(nullValue()));
    assertThat(bounds.getRight(), is(equalTo("5")));
  }

  @Test
  public void testAddBoundaryToQuery_bothBounds() {
    GenericJdbcDatabaseAccessor accessor = new GenericJdbcDatabaseAccessor();
    String result = accessor.addBoundaryToQuery(null, BOUNDARY_SQL, "strategy_id", "3", "5");

    assertThat(result, is(equalTo(
        "SELECT * FROM (" + BOUNDARY_SQL + ") tmptable WHERE \"strategy_id\" >= 3 AND \"strategy_id\" < 5")));
  }

  @Test
  public void testAddBoundaryToQuery_upperOnlyIncludesNulls() {
    GenericJdbcDatabaseAccessor accessor = new GenericJdbcDatabaseAccessor();
    String result = accessor.addBoundaryToQuery(null, BOUNDARY_SQL, "strategy_id", null, "5");

    assertThat(result, is(equalTo(
        "SELECT * FROM (" + BOUNDARY_SQL + ") tmptable WHERE \"strategy_id\" < 5 OR \"strategy_id\" IS NULL")));
  }

  @Test
  public void testAddBoundaryToQuery_lowerOnly() {
    GenericJdbcDatabaseAccessor accessor = new GenericJdbcDatabaseAccessor();
    String result = accessor.addBoundaryToQuery(null, BOUNDARY_SQL, "strategy_id", "3", null);

    assertThat(result, is(equalTo(
        "SELECT * FROM (" + BOUNDARY_SQL + ") tmptable WHERE \"strategy_id\" >= 3")));
  }

  @Test
  public void testAddBoundaryToQuery_rewritesNamedTable() {
    GenericJdbcDatabaseAccessor accessor = new GenericJdbcDatabaseAccessor();
    String result = accessor.addBoundaryToQuery("test_strategy",
        "select * from test_strategy where priority > 0", "strategy_id", "3", "5");

    assertThat(result, is(equalTo(
        "select * from  (SELECT * FROM test_strategy WHERE \"strategy_id\" >= 3 AND \"strategy_id\" < 5)"
            + " tmptable  where priority > 0")));
  }

  @Test
  public void testAddBoundaryToQuery_mysqlAccessorDoesNotQuoteColumn() {
    MySqlDatabaseAccessor accessor = new MySqlDatabaseAccessor();
    String result = accessor.addBoundaryToQuery(null, BOUNDARY_SQL, "strategy_id", "3", "5");

    assertThat(result, is(equalTo(
        "SELECT * FROM (" + BOUNDARY_SQL + ") tmptable WHERE strategy_id >= 3 AND strategy_id < 5")));
  }

  @Test
  public void testNeedColumnQuote_genericAccessor() {
    Configuration conf = buildConfiguration();
    DatabaseAccessor accessor = DatabaseAccessorFactory.getAccessor(conf);

    assertThat(accessor.needColumnQuote(), is(true));
  }

  @Test
  public void testNeedColumnQuote_mysqlAccessor() {
    Configuration conf = buildConfiguration();
    conf.set(JdbcStorageConfig.DATABASE_TYPE.getPropertyName(), "MYSQL");
    DatabaseAccessor accessor = DatabaseAccessorFactory.getAccessor(conf);

    assertThat(accessor.needColumnQuote(), is(false));
  }

  @Test
  public void testClose_afterInitializedAccessor() throws Exception {
    Configuration conf = buildConfiguration();
    DatabaseAccessor accessor = DatabaseAccessorFactory.getAccessor(conf);

    assertThat(accessor.getColumnNames(conf), is(notNullValue()));
    accessor.close();
    accessor.close();
  }

  @Test
  public void testGetRecordWriter_writesRows() throws Exception {
    Configuration conf = buildConfiguration();
    conf.set(JdbcStorageConfig.JDBC_URL.getPropertyName(),
        conf.get(JdbcStorageConfig.JDBC_URL.getPropertyName())
            .replace("jdbc:h2:mem:test;MODE=MySQL;INIT",
                "jdbc:h2:mem:test_writer;MODE=MySQL;DB_CLOSE_DELAY=-1;INIT"));
    conf.set(JdbcStorageConfig.TABLE.getPropertyName(), "test_writer");
    conf.set(serdeConstants.LIST_COLUMNS, "id,name");
    conf.set(serdeConstants.LIST_COLUMN_TYPES, "int,string");

    TaskAttemptContext context = mock(TaskAttemptContext.class);
    when(context.getConfiguration()).thenReturn(conf);

    DatabaseAccessor accessor = DatabaseAccessorFactory.getAccessor(conf);
    @SuppressWarnings("rawtypes")
    RecordWriter writer = accessor.getRecordWriter(context);
    DBRecordWritable record = new DBRecordWritable(2);
    record.set(0, 10);
    record.set(1, "written");

    writer.write(record, null);
    writer.close(context);

    String readBackUrl = conf.get(JdbcStorageConfig.JDBC_URL.getPropertyName()).replaceAll(";INIT=.*", "");
    try (Connection conn = DriverManager.getConnection(readBackUrl);
         Statement statement = conn.createStatement()) {
      try (ResultSet rs = statement.executeQuery("select id, name from test_writer")) {
        assertThat(rs.next(), is(true));
        assertThat(rs.getInt("id"), is(equalTo(10)));
        assertThat(rs.getString("name"), is(equalTo("written")));
        assertThat(rs.next(), is(false));
      }
      statement.execute("SHUTDOWN");
    }
  }

  @Test
  public void testGetRecordWriter_writesMultipleRows() throws Exception {
    Configuration conf = buildConfiguration();
    conf.set(JdbcStorageConfig.JDBC_URL.getPropertyName(),
        conf.get(JdbcStorageConfig.JDBC_URL.getPropertyName())
            .replace("jdbc:h2:mem:test;MODE=MySQL;INIT",
                "jdbc:h2:mem:test_writer_multi;MODE=MySQL;DB_CLOSE_DELAY=-1;INIT"));
    conf.set(JdbcStorageConfig.TABLE.getPropertyName(), "test_writer");
    conf.set(serdeConstants.LIST_COLUMNS, "id,name");
    conf.set(serdeConstants.LIST_COLUMN_TYPES, "int,string");

    TaskAttemptContext context = mock(TaskAttemptContext.class);
    when(context.getConfiguration()).thenReturn(conf);

    DatabaseAccessor accessor = DatabaseAccessorFactory.getAccessor(conf);
    @SuppressWarnings("rawtypes")
    RecordWriter writer = accessor.getRecordWriter(context);

    DBRecordWritable firstRecord = new DBRecordWritable(2);
    firstRecord.set(0, 11);
    firstRecord.set(1, "first");
    writer.write(firstRecord, null);

    DBRecordWritable secondRecord = new DBRecordWritable(2);
    secondRecord.set(0, 12);
    secondRecord.set(1, "second");
    writer.write(secondRecord, null);

    writer.close(context);

    String readBackUrl = conf.get(JdbcStorageConfig.JDBC_URL.getPropertyName()).replaceAll(";INIT=.*", "");
    try (Connection conn = DriverManager.getConnection(readBackUrl);
         Statement statement = conn.createStatement()) {
      try (ResultSet rs = statement.executeQuery("select id, name from test_writer order by id")) {
        assertThat(rs.next(), is(true));
        assertThat(rs.getInt("id"), is(equalTo(11)));
        assertThat(rs.getString("name"), is(equalTo("first")));

        assertThat(rs.next(), is(true));
        assertThat(rs.getInt("id"), is(equalTo(12)));
        assertThat(rs.getString("name"), is(equalTo("second")));

        assertThat(rs.next(), is(false));
      }
      statement.execute("SHUTDOWN");
    }
  }

  private String getStrategyIdColumnName(Configuration conf, DatabaseAccessor accessor)
      throws HiveJdbcDatabaseAccessException {
    return accessor.getColumnNames(conf).get(0);
  }


  private Configuration buildConfiguration() {
    String scriptPath =
        TestGenericJdbcDatabaseAccessor.class.getClassLoader().getResource("test_script.sql")
      .getPath();
    Configuration config = new Configuration();
    config.set(JdbcStorageConfig.DATABASE_TYPE.getPropertyName(), "H2");
    config.set(JdbcStorageConfig.JDBC_DRIVER_CLASS.getPropertyName(), "org.h2.Driver");
    config.set(JdbcStorageConfig.JDBC_URL.getPropertyName(), "jdbc:h2:mem:test;MODE=MySQL;INIT=runscript from '"
        + scriptPath + "'");
    config.set(JdbcStorageConfig.QUERY.getPropertyName(), "select * from test_strategy");
    config.set(serdeConstants.LIST_COLUMNS, "strategy_id,name,referrer,landing,priority,implementation,last_modified");
    config.set(serdeConstants.LIST_COLUMN_TYPES, "int,string,string,string,int,string,timestamp");
    return config;
  }

}
