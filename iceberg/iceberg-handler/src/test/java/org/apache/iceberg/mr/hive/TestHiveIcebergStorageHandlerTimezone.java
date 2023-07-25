/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.mr.hive;

import java.io.IOException;
import java.text.DateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.TimeZone;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.common.DynFields;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.mr.TestHelper;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.junit.runners.Parameterized.Parameter;
import static org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class TestHiveIcebergStorageHandlerTimezone {
  private static final Optional<ThreadLocal<DateFormat>> dateFormat =
      Optional.ofNullable((ThreadLocal<DateFormat>) DynFields.builder()
          .hiddenImpl(TimestampWritable.class, "threadLocalDateFormat")
          .defaultAlwaysNull()
          .buildStatic()
          .get());

  private static final Optional<ThreadLocal<TimeZone>> localTimeZone =
      Optional.ofNullable((ThreadLocal<TimeZone>) DynFields.builder()
          .hiddenImpl(DateWritable.class, "LOCAL_TIMEZONE")
          .defaultAlwaysNull()
          .buildStatic()
          .get());

  @Parameters(name = "timezone={0}")
  public static Collection<Object[]> parameters() {
    return ImmutableList.of(
        new String[] {"America/New_York"},
        new String[] {"Asia/Kolkata"},
        new String[] {"UTC/Greenwich"}
    );
  }

  private static TestHiveShell shell;

  private TestTables testTables;

  @Parameter(0)
  public String timezoneString;

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @BeforeClass
  public static void beforeClass() {
    shell = HiveIcebergStorageHandlerTestUtils.shell();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    shell.stop();
  }

  @Before
  public void before() throws IOException {
    TimeZone.setDefault(TimeZone.getTimeZone(timezoneString));
    TypeInfoFactory.timestampLocalTZTypeInfo.setTimeZone(TimeZone.getTimeZone(timezoneString).toZoneId());

    // Magic to clean cached date format and local timezone for Hive where the default timezone is used/stored in the
    // cached object
    dateFormat.ifPresent(ThreadLocal::remove);
    localTimeZone.ifPresent(ThreadLocal::remove);

    this.testTables = HiveIcebergStorageHandlerTestUtils.testTables(shell, TestTables.TestTableType.HIVE_CATALOG, temp);
    HiveIcebergStorageHandlerTestUtils.init(shell, testTables, temp, "tez");
  }

  @After
  public void after() throws Exception {
    HiveIcebergStorageHandlerTestUtils.close(shell);
  }

  @Test
  public void testDateQuery() throws IOException {
    Schema dateSchema = new Schema(optional(1, "d_date", Types.DateType.get()));

    List<Record> records = TestHelper.RecordsBuilder.newInstance(dateSchema)
        .add(LocalDate.of(2020, 1, 21))
        .add(LocalDate.of(2020, 1, 24))
        .build();

    testTables.createTable(shell, "date_test", dateSchema, FileFormat.PARQUET, records);

    List<Object[]> result = shell.executeStatement("SELECT * from date_test WHERE d_date='2020-01-21'");
    Assert.assertEquals(1, result.size());
    Assert.assertEquals("2020-01-21", result.get(0)[0]);

    result = shell.executeStatement("SELECT * from date_test WHERE d_date in ('2020-01-21', '2020-01-22')");
    Assert.assertEquals(1, result.size());
    Assert.assertEquals("2020-01-21", result.get(0)[0]);

    result = shell.executeStatement("SELECT * from date_test WHERE d_date > '2020-01-21'");
    Assert.assertEquals(1, result.size());
    Assert.assertEquals("2020-01-24", result.get(0)[0]);

    result = shell.executeStatement("SELECT * from date_test WHERE d_date='2020-01-20'");
    Assert.assertEquals(0, result.size());
  }

  @Test
  public void testTimestampQuery() throws IOException {
    Schema timestampSchema = new Schema(optional(1, "d_ts", Types.TimestampType.withoutZone()));

    List<Record> records = TestHelper.RecordsBuilder.newInstance(timestampSchema)
        .add(LocalDateTime.of(2019, 1, 22, 9, 44, 54, 100000000))
        .add(LocalDateTime.of(2019, 2, 22, 9, 44, 54, 200000000))
        .build();

    testTables.createTable(shell, "ts_test", timestampSchema, FileFormat.PARQUET, records);

    List<Object[]> result = shell.executeStatement("SELECT d_ts FROM ts_test WHERE d_ts='2019-02-22 09:44:54.2'");
    Assert.assertEquals(1, result.size());
    Assert.assertEquals("2019-02-22 09:44:54.2", result.get(0)[0]);

    result = shell.executeStatement(
        "SELECT * FROM ts_test WHERE d_ts in ('2017-01-01 22:30:57.1', '2019-02-22 09:44:54.2')");
    Assert.assertEquals(1, result.size());
    Assert.assertEquals("2019-02-22 09:44:54.2", result.get(0)[0]);

    result = shell.executeStatement("SELECT d_ts FROM ts_test WHERE d_ts < '2019-02-22 09:44:54.2'");
    Assert.assertEquals(1, result.size());
    Assert.assertEquals("2019-01-22 09:44:54.1", result.get(0)[0]);

    result = shell.executeStatement("SELECT * FROM ts_test WHERE d_ts='2017-01-01 22:30:57.3'");
    Assert.assertEquals(0, result.size());
  }

  @Test
  public void testTimestampQueryWithTimeZone() throws IOException {
    Schema timestampSchema = new Schema(optional(1, "d_ts", Types.TimestampType.withZone()));

    List<Record> records = TestHelper.RecordsBuilder.newInstance(timestampSchema)
        .add(OffsetDateTime.of(LocalDateTime.of(2019, 1, 22, 9, 44, 54, 100000000), ZoneOffset.of("+00")))
        .add(OffsetDateTime.of(LocalDateTime.of(2019, 2, 22, 9, 44, 54, 200000000), ZoneOffset.of("+00")))
        .build();

    testTables.createTable(shell, "ts_test_tz", timestampSchema, FileFormat.PARQUET, records);

    List<Object[]> result = shell.executeStatement("SELECT d_ts FROM ts_test_tz where d_ts='2019-02-22 09:44:54.200Z'");
    Assert.assertEquals(1, result.size());
    if (timezoneString.equals("America/New_York")) {
      Assert.assertEquals("2019-02-22 04:44:54.2 " + timezoneString, result.get(0)[0]);
    } else if (timezoneString.equals("Asia/Kolkata")) {
      Assert.assertEquals("2019-02-22 15:14:54.2 " + timezoneString, result.get(0)[0]);
    } else if (timezoneString.equals("GMT")) {
      Assert.assertEquals("2019-02-22 09:44:54.2 " + timezoneString, result.get(0)[0]);
    }
  }

  @Test
  public void testFetchTaskWithTimestampWithLocalTimeZone() throws IOException {
    Schema timestampSchema = new Schema(optional(1, "d_ts", Types.TimestampType.withZone()));

    List<Record> records = TestHelper.RecordsBuilder.newInstance(timestampSchema)
        .add(OffsetDateTime.of(LocalDateTime.of(2019, 2, 22, 9, 44, 54, 100000000), ZoneOffset.of("+00")))
        .build();

    testTables.createTable(shell, "ts_test_tz", timestampSchema, FileFormat.PARQUET, records);

    List<Object[]> result = shell.executeStatement("SELECT * FROM ts_test_tz");
    Assert.assertEquals(1, result.size());
    if (timezoneString.equals("America/New_York")) {
      Assert.assertEquals("2019-02-22 04:44:54.1 " + timezoneString, result.get(0)[0]);
    } else if (timezoneString.equals("Asia/Kolkata")) {
      Assert.assertEquals("2019-02-22 15:14:54.1 " + timezoneString, result.get(0)[0]);
    } else if (timezoneString.equals("GMT")) {
      Assert.assertEquals("2019-02-22 09:44:54.1 " + timezoneString, result.get(0)[0]);
    }
  }
}
