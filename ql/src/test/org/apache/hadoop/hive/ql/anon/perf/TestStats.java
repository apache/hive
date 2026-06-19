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

package org.apache.hadoop.hive.ql.anon.perf;

import org.apache.hadoop.hive.metastore.api.ColumnInternalFormat;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

public class TestStats {

  private static final Logger LOG = LoggerFactory.getLogger(TestStats.class);

  @Test
  public void dummy() {
    final int totalMsg = 100_000_000;
    final int[] arrPiiDivider = new int[]{10, 100, 1000};
    final int[] arrUniqueUsers = new int[]{10_000, 100_000};
    final int[] fieldLengths = new int[]{10, 20, 30};
    final int[] arrNumKeys = new int[]{1, 10, 100};

    final AtomicInteger counter = new AtomicInteger(0);
    System.out.printf("%15s%15s%15s%15s%15s%15s\n", "f len", " pii msg", "msg/usr", "num keys", "unq users", "total msg");

    {
      {
        for (final int piiDivider : arrPiiDivider) {
          for (final int uniqueUsers : arrUniqueUsers) {

            final int piiMSg = totalMsg / piiDivider;
            final int msgPerUser = totalMsg / piiDivider / uniqueUsers;

            Assertions.assertEquals(totalMsg, piiDivider * msgPerUser * uniqueUsers);

            final TestContext ctx = new TestContext();
            ctx.internalFormat = ColumnInternalFormat.JSON;
            ctx.numUniqueUsers = uniqueUsers;
            ctx.allToPiiRatio = piiDivider;
            ctx.totalMessages = totalMsg;
            ctx.piiMsgFieldLen = -1;
            ctx.numKeys = -1;

            System.out.printf("%,15d%,15d%,15d%,15d%,15d%,15d\n", -1, piiMSg, msgPerUser, -1, uniqueUsers, totalMsg);
            counter.incrementAndGet();
          }

        }
      }
    }
    System.out.println(counter);
  }

  private void run(final TestContext ctx) {

  }
}
