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

package org.apache.hadoop.hive.ql.exec;

import org.junit.Assert;
import org.junit.Test;

public class UtilitiesTest {

  String s1 = "/data/network/probes/mumbai/probes_userplane_mu_extp_3march_case1/"
      + "partition_date=2021-01-13/hour=4/part-00026-23003837-facb-49ec-b1c4-eeda902cacf3.c000.zlib.orc";
  String s2 = "/data/network/probes/mumbai/probes_userplane_mu_extp_3march_case1/"
      + "partition_date=2021-01-13/hour=4/part-00003-c6acfdee-0c32-492e-b209-c2f1cf477770.c000";

  @Test
  public void TestSparkEmittedFileFormat() {

    Assert.assertEquals("00026", Utilities.getTaskIdFromFilename(s1));
    Assert.assertEquals("00026", Utilities.getPrefixedTaskIdFromFilename(s1));
    Assert.assertEquals(1, Utilities.getAttemptIdFromFilename(s1));

    Assert.assertEquals("00003", Utilities.getTaskIdFromFilename(s2));
    Assert.assertEquals("00003", Utilities.getPrefixedTaskIdFromFilename(s2));
    Assert.assertEquals(1, Utilities.getAttemptIdFromFilename(s2));

  }
}
