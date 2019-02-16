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
package org.apache.hadoop.hive.druid;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.druid.io.DruidQueryBasedInputFormat;
import org.apache.hadoop.hive.druid.io.HiveDruidSplit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * This input format adds a faulty host as first input split location.
 * Tests should be able to query results successfully by trying next split locations.
 */
public class QTestDruidQueryBasedInputFormatToAddFaultyHost extends DruidQueryBasedInputFormat {

  @Override
  protected HiveDruidSplit[] getInputSplits(Configuration conf) throws IOException {
    HiveDruidSplit[] inputSplits = super.getInputSplits(conf);
    List<HiveDruidSplit> list = new ArrayList<>();
    for(HiveDruidSplit split : inputSplits) {
      String[] locations = split.getLocations();
      List<String> locationsWithFaultyHost = Lists.newArrayListWithCapacity(locations.length + 1);
      // A non-queryable host location.
      locationsWithFaultyHost.add("localhost:8081");
      locationsWithFaultyHost.addAll(Arrays.asList(locations));
      HiveDruidSplit hiveDruidSplit = new HiveDruidSplit(split.getDruidQuery(), split.getPath(),
              locationsWithFaultyHost.toArray(new String[0])
      );
      list.add(hiveDruidSplit);
    }
    return list.toArray(new HiveDruidSplit[0]);
  }
}
