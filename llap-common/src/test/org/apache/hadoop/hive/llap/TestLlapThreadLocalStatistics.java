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
package org.apache.hadoop.hive.llap;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Assert;
import org.junit.Test;

import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TestLlapThreadLocalStatistics {

  private static final ThreadMXBean mxBean = LlapUtil.initThreadMxBean();
  private static final String FILE = "file";
  private static final String HDFS = "hdfs";

  @Test
  public void testEmptyStatistics() {
    LlapThreadLocalStatistics before = new LlapThreadLocalStatistics(mxBean, new ArrayList<>());
    LlapThreadLocalStatistics after = new LlapThreadLocalStatistics(mxBean, new ArrayList<>());
    Assert.assertEquals(0, after.subtract(before).schemeToThreadLocalStats.keySet().size());
  }

  @Test
  public void testCpuTimeUserTime() throws Exception {
    LlapThreadLocalStatistics before = new LlapThreadLocalStatistics(mxBean, new ArrayList<>());
    Assert.assertTrue("cpuTime should be >0", before.cpuTime > 0);
    Assert.assertTrue("userTime should be >0", before.userTime > 0);

    Thread.sleep(100);

    LlapThreadLocalStatistics after = new LlapThreadLocalStatistics(mxBean, new ArrayList<>());
    Assert.assertTrue("cpuTime should increase", after.cpuTime > before.cpuTime);
    // userTime assertion is flaky, not checked here
  }

  @Test
  public void testCountersMergedForTheSameScheme() {
    LlapThreadLocalStatistics stats = new LlapThreadLocalStatistics(mxBean,
        createMockStatistics(new String[]{FILE, HDFS, HDFS}, new Integer[]{1, 1, 1}));
    Assert.assertEquals(1, stats.schemeToThreadLocalStats.get(FILE).bytesRead);
    Assert.assertEquals(2, stats.schemeToThreadLocalStats.get(HDFS).bytesRead);
  }

  @Test
  public void testCountersBeforeAfter() {
    LlapThreadLocalStatistics before = new LlapThreadLocalStatistics(mxBean,
        createMockStatistics(new String[]{FILE, HDFS, HDFS}, new Integer[]{1, 1, 1}));
    LlapThreadLocalStatistics after = new LlapThreadLocalStatistics(mxBean,
        createMockStatistics(new String[]{FILE, HDFS, HDFS}, new Integer[]{3, 1, 4}));

    Assert.assertEquals(1, before.schemeToThreadLocalStats.get(FILE).bytesRead);
    Assert.assertEquals(2, before.schemeToThreadLocalStats.get(HDFS).bytesRead);
    Assert.assertEquals(3, after.schemeToThreadLocalStats.get(FILE).bytesRead);
    Assert.assertEquals(5, after.schemeToThreadLocalStats.get(HDFS).bytesRead);

    after.subtract(before);

    // file: 3 - 1
    Assert.assertEquals(2, after.schemeToThreadLocalStats.get(FILE).bytesRead);
    // hdfs: (1 + 4) - (1 + 1)
    Assert.assertEquals(3, after.schemeToThreadLocalStats.get(HDFS).bytesRead);
  }

  private List<FileSystem.Statistics> createMockStatistics(String[] schemes, Integer[] values) {
    return IntStream.range(0, schemes.length).mapToObj(i -> {
      FileSystem.Statistics stat = new FileSystem.Statistics(schemes[i]);
      stat.incrementBytesRead(values[i]);
      return stat;
    }).collect(Collectors.toList());
  }
}
