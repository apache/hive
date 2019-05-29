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

package org.apache.hadoop.hive.metastore.columnstats.merge;

import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Date;
import org.apache.hadoop.hive.metastore.columnstats.cache.DateColumnStatsDataInspector;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MetastoreUnitTest.class)
public class DateColumnStatsMergerTest {

  private static final Date DATE_1 = new Date(1);
  private static final Date DATE_2 = new Date(2);
  private static final Date DATE_3 = new Date(3);

  private ColumnStatsMerger merger = new DateColumnStatsMerger();

  @Test
  public void testMergeNullMinMaxValues() {
    ColumnStatisticsObj old = new ColumnStatisticsObj();
    createData(old, null, null);

    merger.merge(old, old);

    Assert.assertNull(old.getStatsData().getDateStats().getLowValue());
    Assert.assertNull(old.getStatsData().getDateStats().getHighValue());
  }

  @Test
  public void testMergeNulls() {
    ColumnStatisticsObj oldObj = new ColumnStatisticsObj();
    createData(oldObj, null, null);

    ColumnStatisticsObj newObj;

    newObj = new ColumnStatisticsObj();
    createData(newObj, null, null);
    merger.merge(oldObj, newObj);

    Assert.assertEquals(null, oldObj.getStatsData().getDateStats().getLowValue());
    Assert.assertEquals(null, oldObj.getStatsData().getDateStats().getHighValue());

    newObj = new ColumnStatisticsObj();
    createData(newObj, DATE_1, DATE_3);
    merger.merge(oldObj, newObj);

    newObj = new ColumnStatisticsObj();
    createData(newObj, null, null);
    merger.merge(oldObj, newObj);

    Assert.assertEquals(DATE_1, oldObj.getStatsData().getDateStats().getLowValue());
    Assert.assertEquals(DATE_3, oldObj.getStatsData().getDateStats().getHighValue());
  }

  @Test
  public void testMergeNonNullAndNullLowerValuesNewIsNull() {
    ColumnStatisticsObj oldObj = new ColumnStatisticsObj();
    createData(oldObj, DATE_2, DATE_2);

    ColumnStatisticsObj newObj;

    newObj = new ColumnStatisticsObj();
    createData(newObj, DATE_3, DATE_3);
    merger.merge(oldObj, newObj);

    newObj = new ColumnStatisticsObj();
    createData(newObj, DATE_1, DATE_1);
    merger.merge(oldObj, newObj);

    Assert.assertEquals(DATE_1, oldObj.getStatsData().getDateStats().getLowValue());
    Assert.assertEquals(DATE_3, oldObj.getStatsData().getDateStats().getHighValue());
  }

  private DateColumnStatsDataInspector createData(ColumnStatisticsObj objNulls, Date lowValue,
      Date highValue) {
    ColumnStatisticsData statisticsData = new ColumnStatisticsData();
    DateColumnStatsDataInspector data = new DateColumnStatsDataInspector();

    statisticsData.setDateStats(data);
    objNulls.setStatsData(statisticsData);

    data.setLowValue(lowValue);
    data.setHighValue(highValue);
    return data;
  }
}
