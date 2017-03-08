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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.equalToIgnoringCase;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import org.apache.hive.storage.jdbc.conf.JdbcStorageConfig;
import org.apache.hive.storage.jdbc.exception.HiveJdbcDatabaseAccessException;

import java.util.List;
import java.util.Map;

public class GenericJdbcDatabaseAccessorTest {

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
  public void testGetColumnNames_fieldListQuery() throws HiveJdbcDatabaseAccessException {
    Configuration conf = buildConfiguration();
    conf.set(JdbcStorageConfig.QUERY.getPropertyName(), "select name,referrer from test_strategy");
    DatabaseAccessor accessor = DatabaseAccessorFactory.getAccessor(conf);
    List<String> columnNames = accessor.getColumnNames(conf);

    assertThat(columnNames, is(notNullValue()));
    assertThat(columnNames.size(), is(equalTo(2)));
    assertThat(columnNames.get(0), is(equalToIgnoringCase("name")));
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
    JdbcRecordIterator iterator = accessor.getRecordIterator(conf, 2, 0);

    assertThat(iterator, is(notNullValue()));

    int count = 0;
    while (iterator.hasNext()) {
      Map<String, String> record = iterator.next();
      count++;

      assertThat(record, is(notNullValue()));
      assertThat(record.size(), is(equalTo(7)));
      assertThat(record.get("STRATEGY_ID"), is(equalTo(String.valueOf(count))));
    }

    assertThat(count, is(equalTo(2)));
    iterator.close();
  }


  @Test
  public void testGetRecordIterator_offsets() throws HiveJdbcDatabaseAccessException {
    Configuration conf = buildConfiguration();
    DatabaseAccessor accessor = DatabaseAccessorFactory.getAccessor(conf);
    JdbcRecordIterator iterator = accessor.getRecordIterator(conf, 2, 2);

    assertThat(iterator, is(notNullValue()));

    int count = 0;
    while (iterator.hasNext()) {
      Map<String, String> record = iterator.next();
      count++;

      assertThat(record, is(notNullValue()));
      assertThat(record.size(), is(equalTo(7)));
      assertThat(record.get("STRATEGY_ID"), is(equalTo(String.valueOf(count + 2))));
    }

    assertThat(count, is(equalTo(2)));
    iterator.close();
  }


  @Test
  public void testGetRecordIterator_emptyResultSet() throws HiveJdbcDatabaseAccessException {
    Configuration conf = buildConfiguration();
    conf.set(JdbcStorageConfig.QUERY.getPropertyName(), "select * from test_strategy where strategy_id = '25'");
    DatabaseAccessor accessor = DatabaseAccessorFactory.getAccessor(conf);
    JdbcRecordIterator iterator = accessor.getRecordIterator(conf, 0, 2);

    assertThat(iterator, is(notNullValue()));
    assertThat(iterator.hasNext(), is(false));
    iterator.close();
  }


  @Test
  public void testGetRecordIterator_largeOffset() throws HiveJdbcDatabaseAccessException {
    Configuration conf = buildConfiguration();
    DatabaseAccessor accessor = DatabaseAccessorFactory.getAccessor(conf);
    JdbcRecordIterator iterator = accessor.getRecordIterator(conf, 10, 25);

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
      JdbcRecordIterator iterator = accessor.getRecordIterator(conf, 0, 2);
  }


  private Configuration buildConfiguration() {
    String scriptPath =
      GenericJdbcDatabaseAccessorTest.class.getClassLoader().getResource("test_script.sql")
      .getPath();
    Configuration config = new Configuration();
    config.set(JdbcStorageConfig.DATABASE_TYPE.getPropertyName(), "H2");
    config.set(JdbcStorageConfig.JDBC_DRIVER_CLASS.getPropertyName(), "org.h2.Driver");
    config.set(JdbcStorageConfig.JDBC_URL.getPropertyName(), "jdbc:h2:mem:test;MODE=MySQL;INIT=runscript from '"
        + scriptPath + "'");
    config.set(JdbcStorageConfig.QUERY.getPropertyName(), "select * from test_strategy");

    return config;
  }

}
