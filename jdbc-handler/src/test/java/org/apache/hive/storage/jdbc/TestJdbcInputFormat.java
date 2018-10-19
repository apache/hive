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
package org.apache.hive.storage.jdbc;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hive.storage.jdbc.dao.DatabaseAccessor;
import org.apache.hive.storage.jdbc.dao.DatabaseAccessorFactory;
import org.apache.hive.storage.jdbc.exception.HiveJdbcDatabaseAccessException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.BDDMockito;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest(DatabaseAccessorFactory.class)
public class TestJdbcInputFormat {

  @Mock
  private DatabaseAccessor mockDatabaseAccessor;


  @Test
  public void testLimitSplit_noSpillOver() throws HiveJdbcDatabaseAccessException, IOException {
    PowerMockito.mockStatic(DatabaseAccessorFactory.class);
    BDDMockito.given(DatabaseAccessorFactory.getAccessor(any(Configuration.class))).willReturn(mockDatabaseAccessor);
    JdbcInputFormat f = new JdbcInputFormat();
    when(mockDatabaseAccessor.getTotalNumberOfRecords(any(Configuration.class))).thenReturn(15);

    JobConf conf = new JobConf();
    conf.set("mapred.input.dir", "/temp");
    conf.set("hive.sql.numPartitions", "3");
    InputSplit[] splits = f.getSplits(conf, -1);

    assertThat(splits, is(notNullValue()));
    assertThat(splits.length, is(3));

    assertThat(splits[0].getLength(), is(5L));
  }


  @Test
  public void testLimitSplit_withSpillOver() throws HiveJdbcDatabaseAccessException, IOException {
    PowerMockito.mockStatic(DatabaseAccessorFactory.class);
    BDDMockito.given(DatabaseAccessorFactory.getAccessor(any(Configuration.class))).willReturn(mockDatabaseAccessor);
    JdbcInputFormat f = new JdbcInputFormat();
    when(mockDatabaseAccessor.getTotalNumberOfRecords(any(Configuration.class))).thenReturn(15);

    JobConf conf = new JobConf();
    conf.set("mapred.input.dir", "/temp");
    conf.set("hive.sql.numPartitions", "6");
    InputSplit[] splits = f.getSplits(conf, -1);

    assertThat(splits, is(notNullValue()));
    assertThat(splits.length, is(6));

    for (int i = 0; i < 3; i++) {
      assertThat(splits[i].getLength(), is(3L));
    }

    for (int i = 3; i < 6; i++) {
      assertThat(splits[i].getLength(), is(2L));
    }
  }

  @Test
  public void testIntervalSplit_Long() throws HiveJdbcDatabaseAccessException, IOException {
    PowerMockito.mockStatic(DatabaseAccessorFactory.class);
    BDDMockito.given(DatabaseAccessorFactory.getAccessor(any(Configuration.class))).willReturn(mockDatabaseAccessor);
    JdbcInputFormat f = new JdbcInputFormat();
    when(mockDatabaseAccessor.getColumnNames(any(Configuration.class))).thenReturn(Lists.newArrayList("a"));

    JobConf conf = new JobConf();
    conf.set("mapred.input.dir", "/temp");
    conf.set(serdeConstants.LIST_COLUMN_TYPES, "int");
    conf.set("hive.sql.partitionColumn", "a");
    conf.set("hive.sql.numPartitions", "3");
    conf.set("hive.sql.lowerBound", "1");
    conf.set("hive.sql.upperBound", "10");
    InputSplit[] splits = f.getSplits(conf, -1);

    assertThat(splits, is(notNullValue()));
    assertThat(splits.length, is(3));

    assertNull(((JdbcInputSplit)splits[0]).getLowerBound());
    assertEquals(((JdbcInputSplit)splits[0]).getUpperBound(), "4");
    assertEquals(((JdbcInputSplit)splits[1]).getLowerBound(), "4");
    assertEquals(((JdbcInputSplit)splits[1]).getUpperBound(), "7");
    assertEquals(((JdbcInputSplit)splits[2]).getLowerBound(), "7");
    assertNull(((JdbcInputSplit)splits[2]).getUpperBound());
  }

  @Test
  public void testIntervalSplit_Double() throws HiveJdbcDatabaseAccessException, IOException {
    PowerMockito.mockStatic(DatabaseAccessorFactory.class);
    BDDMockito.given(DatabaseAccessorFactory.getAccessor(any(Configuration.class))).willReturn(mockDatabaseAccessor);
    JdbcInputFormat f = new JdbcInputFormat();
    when(mockDatabaseAccessor.getColumnNames(any(Configuration.class))).thenReturn(Lists.newArrayList("a"));

    JobConf conf = new JobConf();
    conf.set("mapred.input.dir", "/temp");
    conf.set(serdeConstants.LIST_COLUMN_TYPES, "double");
    conf.set("hive.sql.partitionColumn", "a");
    conf.set("hive.sql.numPartitions", "3");
    conf.set("hive.sql.lowerBound", "0");
    conf.set("hive.sql.upperBound", "10");
    InputSplit[] splits = f.getSplits(conf, -1);

    assertThat(splits, is(notNullValue()));
    assertThat(splits.length, is(3));

    assertNull(((JdbcInputSplit)splits[0]).getLowerBound());
    assertTrue(Double.parseDouble(((JdbcInputSplit)splits[0]).getUpperBound()) > 3.3 && Double.parseDouble((
            (JdbcInputSplit)splits[0]).getUpperBound()) < 3.4);
    assertTrue(Double.parseDouble(((JdbcInputSplit)splits[1]).getLowerBound()) > 3.3 && Double.parseDouble((
            (JdbcInputSplit)splits[1]).getLowerBound()) < 3.4);
    assertTrue(Double.parseDouble(((JdbcInputSplit)splits[1]).getUpperBound()) > 6.6 && Double.parseDouble((
            (JdbcInputSplit)splits[1]).getUpperBound()) < 6.7);
    assertTrue(Double.parseDouble(((JdbcInputSplit)splits[2]).getLowerBound()) > 6.6 && Double.parseDouble((
            (JdbcInputSplit)splits[2]).getLowerBound()) < 6.7);
    assertNull(((JdbcInputSplit)splits[2]).getUpperBound());
  }

  @Test
  public void testIntervalSplit_Decimal() throws HiveJdbcDatabaseAccessException, IOException {
    PowerMockito.mockStatic(DatabaseAccessorFactory.class);
    BDDMockito.given(DatabaseAccessorFactory.getAccessor(any(Configuration.class))).willReturn(mockDatabaseAccessor);
    JdbcInputFormat f = new JdbcInputFormat();
    when(mockDatabaseAccessor.getColumnNames(any(Configuration.class))).thenReturn(Lists.newArrayList("a"));

    JobConf conf = new JobConf();
    conf.set("mapred.input.dir", "/temp");
    conf.set(serdeConstants.LIST_COLUMN_TYPES, "decimal(10,5)");
    conf.set("hive.sql.partitionColumn", "a");
    conf.set("hive.sql.numPartitions", "4");
    conf.set("hive.sql.lowerBound", "5");
    conf.set("hive.sql.upperBound", "1000");
    InputSplit[] splits = f.getSplits(conf, -1);

    assertThat(splits, is(notNullValue()));
    assertThat(splits.length, is(4));

    assertNull(((JdbcInputSplit)splits[0]).getLowerBound());
    assertEquals(((JdbcInputSplit)splits[0]).getUpperBound(), "253.75000");
    assertEquals(((JdbcInputSplit)splits[1]).getLowerBound(), "253.75000");
    assertEquals(((JdbcInputSplit)splits[1]).getUpperBound(), "502.50000");
    assertEquals(((JdbcInputSplit)splits[2]).getLowerBound(), "502.50000");
    assertEquals(((JdbcInputSplit)splits[2]).getUpperBound(), "751.25000");
    assertEquals(((JdbcInputSplit)splits[3]).getLowerBound(), "751.25000");
    assertNull(((JdbcInputSplit)splits[3]).getUpperBound());
  }

  @Test
  public void testIntervalSplit_Timestamp() throws HiveJdbcDatabaseAccessException, IOException {
    PowerMockito.mockStatic(DatabaseAccessorFactory.class);
    BDDMockito.given(DatabaseAccessorFactory.getAccessor(any(Configuration.class))).willReturn(mockDatabaseAccessor);
    JdbcInputFormat f = new JdbcInputFormat();
    when(mockDatabaseAccessor.getColumnNames(any(Configuration.class))).thenReturn(Lists.newArrayList("a"));
    when(mockDatabaseAccessor.getBounds(any(Configuration.class), any(String.class), anyBoolean(), anyBoolean()))
            .thenReturn(new ImmutablePair<String, String>("2010-01-01 00:00:00.000000000", "2018-01-01 " +
            "12:00:00.000000000"));

    JobConf conf = new JobConf();
    conf.set("mapred.input.dir", "/temp");
    conf.set(serdeConstants.LIST_COLUMN_TYPES, "timestamp");
    conf.set("hive.sql.partitionColumn", "a");
    conf.set("hive.sql.numPartitions", "2");
    InputSplit[] splits = f.getSplits(conf, -1);

    assertThat(splits, is(notNullValue()));
    assertThat(splits.length, is(2));

    assertNull(((JdbcInputSplit)splits[0]).getLowerBound());
    assertEquals(((JdbcInputSplit)splits[0]).getUpperBound(), "2014-01-01 06:00:00.0");
    assertEquals(((JdbcInputSplit)splits[1]).getLowerBound(), "2014-01-01 06:00:00.0");
    assertNull(((JdbcInputSplit)splits[1]).getUpperBound());
  }

  @Test
  public void testIntervalSplit_Date() throws HiveJdbcDatabaseAccessException, IOException {
    PowerMockito.mockStatic(DatabaseAccessorFactory.class);
    BDDMockito.given(DatabaseAccessorFactory.getAccessor(any(Configuration.class))).willReturn(mockDatabaseAccessor);
    JdbcInputFormat f = new JdbcInputFormat();
    when(mockDatabaseAccessor.getColumnNames(any(Configuration.class))).thenReturn(Lists.newArrayList("a"));
    when(mockDatabaseAccessor.getBounds(any(Configuration.class), any(String.class), anyBoolean(), anyBoolean()))
            .thenReturn(new ImmutablePair<String, String>("2010-01-01", "2018-01-01"));

    JobConf conf = new JobConf();
    conf.set("mapred.input.dir", "/temp");
    conf.set(serdeConstants.LIST_COLUMN_TYPES, "date");
    conf.set("hive.sql.partitionColumn", "a");
    conf.set("hive.sql.numPartitions", "3");
    InputSplit[] splits = f.getSplits(conf, -1);

    assertThat(splits, is(notNullValue()));
    assertThat(splits.length, is(3));

    assertNull(((JdbcInputSplit)splits[0]).getLowerBound());
    assertEquals(((JdbcInputSplit)splits[0]).getUpperBound(), "2012-09-01");
    assertEquals(((JdbcInputSplit)splits[1]).getLowerBound(), "2012-09-01");
    assertEquals(((JdbcInputSplit)splits[1]).getUpperBound(), "2015-05-03");
    assertEquals(((JdbcInputSplit)splits[2]).getLowerBound(), "2015-05-03");
    assertNull(((JdbcInputSplit)splits[2]).getUpperBound());
  }

  @Test
  public void testIntervalSplit_AutoShrink() throws HiveJdbcDatabaseAccessException, IOException {
    PowerMockito.mockStatic(DatabaseAccessorFactory.class);
    BDDMockito.given(DatabaseAccessorFactory.getAccessor(any(Configuration.class))).willReturn(mockDatabaseAccessor);
    JdbcInputFormat f = new JdbcInputFormat();
    when(mockDatabaseAccessor.getColumnNames(any(Configuration.class))).thenReturn(Lists.newArrayList("a"));

    JobConf conf = new JobConf();
    conf.set("mapred.input.dir", "/temp");
    conf.set(serdeConstants.LIST_COLUMN_TYPES, "int");
    conf.set("hive.sql.partitionColumn", "a");
    conf.set("hive.sql.numPartitions", "5");
    conf.set("hive.sql.lowerBound", "2");
    conf.set("hive.sql.upperBound", "4");
    InputSplit[] splits = f.getSplits(conf, -1);

    assertThat(splits, is(notNullValue()));
    assertThat(splits.length, is(2));

    assertNull(((JdbcInputSplit)splits[0]).getLowerBound());
    assertEquals(((JdbcInputSplit)splits[0]).getUpperBound(), "3");
    assertEquals(((JdbcInputSplit)splits[1]).getLowerBound(), "3");
    assertNull(((JdbcInputSplit)splits[1]).getUpperBound());
  }

  @Test
  public void testIntervalSplit_NoSplit() throws HiveJdbcDatabaseAccessException, IOException {
    PowerMockito.mockStatic(DatabaseAccessorFactory.class);
    BDDMockito.given(DatabaseAccessorFactory.getAccessor(any(Configuration.class))).willReturn(mockDatabaseAccessor);
    JdbcInputFormat f = new JdbcInputFormat();
    when(mockDatabaseAccessor.getColumnNames(any(Configuration.class))).thenReturn(Lists.newArrayList("a"));

    JobConf conf = new JobConf();
    conf.set("mapred.input.dir", "/temp");
    conf.set(serdeConstants.LIST_COLUMN_TYPES, "int");
    conf.set("hive.sql.partitionColumn", "a");
    conf.set("hive.sql.numPartitions", "5");
    conf.set("hive.sql.lowerBound", "1");
    conf.set("hive.sql.upperBound", "2");
    InputSplit[] splits = f.getSplits(conf, -1);

    assertThat(splits, is(notNullValue()));
    assertThat(splits.length, is(1));

    assertNull(((JdbcInputSplit)splits[0]).getPartitionColumn());
  }
}
