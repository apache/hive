/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore.utils;

import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.util.TimeZone;

import static org.junit.Assert.assertEquals;

@Category(MetastoreUnitTest.class)
public class TestMetaStoreUtils {

  @Test
  public void testConvertDateToString() {
    TimeZone defaultTZ = TimeZone.getDefault();
    try {
      TimeZone.setDefault(TimeZone.getTimeZone(ZoneId.of("Asia/Hong_Kong")));
      String date = MetaStoreUtils.convertDateToString(Date.valueOf("2023-01-01"));
      assertEquals("2023-01-01", date);
    } finally {
      TimeZone.setDefault(defaultTZ);
    }
  }

  @Test
  public void testcConvertTimestampToString() {
    TimeZone defaultTZ = TimeZone.getDefault();
    try {
      String date = MetaStoreUtils.convertTimestampToString(Timestamp.valueOf("2023-01-01 10:20:30"));
      assertEquals("2023-01-01 10:20:30", date);
    } finally {
      TimeZone.setDefault(defaultTZ);
    }
  }

  @Test
  public void testConvertStringToDate() {
    TimeZone defaultTZ = TimeZone.getDefault();
    try {
      Date date = MetaStoreUtils.convertStringToDate("2023-01-01");
      assertEquals(Date.valueOf("2023-01-01"), date);
    } finally {
      TimeZone.setDefault(defaultTZ);
    }
  }

  @Test
  public void testConvertStringToTimestamp() {
    TimeZone defaultTZ = TimeZone.getDefault();
    try {
      Timestamp date = MetaStoreUtils.convertStringToTimestamp("2023-01-01 10:20:30");
      assertEquals(Timestamp.valueOf("2023-01-01 10:20:30"), date);
    } finally {
      TimeZone.setDefault(defaultTZ);
    }
  }
}
