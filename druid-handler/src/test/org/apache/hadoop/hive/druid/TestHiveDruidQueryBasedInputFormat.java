/**
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
package org.apache.hadoop.hive.druid;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.druid.io.DruidQueryBasedInputFormat;
import org.joda.time.Interval;
import org.joda.time.chrono.ISOChronology;
import org.junit.Test;

import junit.framework.TestCase;

public class TestHiveDruidQueryBasedInputFormat extends TestCase {

  @SuppressWarnings("unchecked")
  @Test
  public void testCreateSplitsIntervals() throws Exception {
    DruidQueryBasedInputFormat input = new DruidQueryBasedInputFormat();

    Method method1 = DruidQueryBasedInputFormat.class.getDeclaredMethod("createSplitsIntervals",
            List.class, int.class
    );
    method1.setAccessible(true);

    List<Interval> intervals;
    List<List<Interval>> resultList;
    List<List<Interval>> expectedResultList;

    // Test 1 : single split, create 4
    intervals = new ArrayList<>();
    intervals.add(new Interval(1262304000000L, 1293840000000L, ISOChronology.getInstanceUTC()));
    resultList = (List<List<Interval>>) method1.invoke(input, intervals, 4);
    expectedResultList = new ArrayList<>();
    expectedResultList.add(Arrays
            .asList(new Interval(1262304000000L, 1270188000000L, ISOChronology.getInstanceUTC())));
    expectedResultList.add(Arrays
            .asList(new Interval(1270188000000L, 1278072000000L, ISOChronology.getInstanceUTC())));
    expectedResultList.add(Arrays
            .asList(new Interval(1278072000000L, 1285956000000L, ISOChronology.getInstanceUTC())));
    expectedResultList.add(Arrays
            .asList(new Interval(1285956000000L, 1293840000000L, ISOChronology.getInstanceUTC())));
    assertEquals(expectedResultList, resultList);

    // Test 2 : two splits, create 4
    intervals = new ArrayList<>();
    intervals.add(new Interval(1262304000000L, 1293840000000L, ISOChronology.getInstanceUTC()));
    intervals.add(new Interval(1325376000000L, 1356998400000L, ISOChronology.getInstanceUTC()));
    resultList = (List<List<Interval>>) method1.invoke(input, intervals, 4);
    expectedResultList = new ArrayList<>();
    expectedResultList.add(Arrays
            .asList(new Interval(1262304000000L, 1278093600000L, ISOChronology.getInstanceUTC())));
    expectedResultList.add(Arrays
            .asList(new Interval(1278093600000L, 1293840000000L, ISOChronology.getInstanceUTC()),
                    new Interval(1325376000000L, 1325419200000L, ISOChronology.getInstanceUTC())
            ));
    expectedResultList.add(Arrays
            .asList(new Interval(1325419200000L, 1341208800000L, ISOChronology.getInstanceUTC())));
    expectedResultList.add(Arrays
            .asList(new Interval(1341208800000L, 1356998400000L, ISOChronology.getInstanceUTC())));
    assertEquals(expectedResultList, resultList);

    // Test 3 : two splits, create 5
    intervals = new ArrayList<>();
    intervals.add(new Interval(1262304000000L, 1293840000000L, ISOChronology.getInstanceUTC()));
    intervals.add(new Interval(1325376000000L, 1356998400000L, ISOChronology.getInstanceUTC()));
    resultList = (List<List<Interval>>) method1.invoke(input, intervals, 5);
    expectedResultList = new ArrayList<>();
    expectedResultList.add(Arrays
            .asList(new Interval(1262304000000L, 1274935680000L, ISOChronology.getInstanceUTC())));
    expectedResultList.add(Arrays
            .asList(new Interval(1274935680000L, 1287567360000L, ISOChronology.getInstanceUTC())));
    expectedResultList.add(Arrays
            .asList(new Interval(1287567360000L, 1293840000000L, ISOChronology.getInstanceUTC()),
                    new Interval(1325376000000L, 1331735040000L, ISOChronology.getInstanceUTC())
            ));
    expectedResultList.add(Arrays
            .asList(new Interval(1331735040000L, 1344366720000L, ISOChronology.getInstanceUTC())));
    expectedResultList.add(Arrays
            .asList(new Interval(1344366720000L, 1356998400000L, ISOChronology.getInstanceUTC())));
    assertEquals(expectedResultList, resultList);

    // Test 4 : three splits, different ranges, create 6
    intervals = new ArrayList<>();
    intervals.add(new Interval(1199145600000L, 1201824000000L,
            ISOChronology.getInstanceUTC()
    )); // one month
    intervals.add(new Interval(1325376000000L, 1356998400000L,
            ISOChronology.getInstanceUTC()
    )); // one year
    intervals.add(new Interval(1407283200000L, 1407888000000L,
            ISOChronology.getInstanceUTC()
    )); // 7 days
    resultList = (List<List<Interval>>) method1.invoke(input, intervals, 6);
    expectedResultList = new ArrayList<>();
    expectedResultList.add(Arrays
            .asList(new Interval(1199145600000L, 1201824000000L, ISOChronology.getInstanceUTC()),
                    new Interval(1325376000000L, 1328515200000L, ISOChronology.getInstanceUTC())
            ));
    expectedResultList.add(Arrays
            .asList(new Interval(1328515200000L, 1334332800000L, ISOChronology.getInstanceUTC())));
    expectedResultList.add(Arrays
            .asList(new Interval(1334332800000L, 1340150400000L, ISOChronology.getInstanceUTC())));
    expectedResultList.add(Arrays
            .asList(new Interval(1340150400000L, 1345968000000L, ISOChronology.getInstanceUTC())));
    expectedResultList.add(Arrays
            .asList(new Interval(1345968000000L, 1351785600000L, ISOChronology.getInstanceUTC())));
    expectedResultList.add(Arrays
            .asList(new Interval(1351785600000L, 1356998400000L, ISOChronology.getInstanceUTC()),
                    new Interval(1407283200000L, 1407888000000L, ISOChronology.getInstanceUTC())
            ));
    assertEquals(expectedResultList, resultList);
  }

}
