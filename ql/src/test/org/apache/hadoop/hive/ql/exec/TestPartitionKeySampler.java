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



import java.util.Arrays;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 * PartitionKeySampler Test.
 */
public class TestPartitionKeySampler {

  private static final byte[] tmp100 = "100".getBytes();
  private static final byte[] tmp200 = "200".getBytes();
  private static final byte[] tmp300 = "300".getBytes();
  private static final byte[] tmp400 = "400".getBytes();

  // current random sampling implementation in InputSampler always returns
  // value of index 3, 5, 8, which can be same with previous partition key.
  // That induces "Split points are out of order" exception in TotalOrderPartitioner causing HIVE-7699
  @Test
  public void test() throws Throwable {
    byte[][] sampled;
    sampled = new byte[][] {
        tmp100, tmp100, tmp100, tmp100, tmp100, tmp100, tmp100, tmp100, tmp100, tmp100
    };
    assertKeys(sampled, tmp100); // 3

    sampled = new byte[][] {
        tmp100, tmp100, tmp100, tmp100, tmp100, tmp100, tmp100, tmp100, tmp200, tmp200
    };
    assertKeys(sampled, tmp100, tmp200); // 3, 8

    sampled = new byte[][] {
        tmp100, tmp100, tmp100, tmp100, tmp200, tmp200, tmp200, tmp300, tmp300, tmp300
    };
    assertKeys(sampled, tmp100, tmp200, tmp300); // 3, 5, 8

    sampled = new byte[][] {
        tmp100, tmp200, tmp200, tmp200, tmp200, tmp200, tmp200, tmp300, tmp300, tmp400
    };
    assertKeys(sampled, tmp200, tmp300, tmp400); // 3, 7, 9

    sampled = new byte[][] {
        tmp100, tmp200, tmp300, tmp400, tmp400, tmp400, tmp400, tmp400, tmp400, tmp400
    };
    assertKeys(sampled, tmp400);  // 3
  }

  private void assertKeys(byte[][] sampled, byte[]... expected) {
    byte[][] keys = PartitionKeySampler.toPartitionKeys(sampled, 4);
    assertEquals(expected.length, keys.length);
    for (int i = 0; i < expected.length; i++) {
      assertTrue(Arrays.equals(expected[i], keys[i]));
    }
  }
}
