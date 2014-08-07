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
package org.apache.hadoop.hive.ql.exec.spark;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.io.BytesWritable;
import org.junit.Test;
import scala.Tuple2;

import static org.junit.Assert.assertTrue;

public class TestHiveKVResultCache {
  @Test
  public void testSimple() throws Exception {
    // Create KV result cache object, add one (k,v) pair and retrieve them.
    HiveConf conf = new HiveConf();
    HiveKVResultCache cache = new HiveKVResultCache(conf);

    BytesWritable key = new BytesWritable("key".getBytes());
    BytesWritable value = new BytesWritable("value".getBytes());
    cache.add(key, value);

    assertTrue("KV result cache should have at least one element", cache.hasNext());

    Tuple2<BytesWritable, BytesWritable> row = cache.next();
    assertTrue("Incorrect key", row._1().equals(key));
    assertTrue("Incorrect value", row._2().equals(value));

    assertTrue("Cache shouldn't have more records", !cache.hasNext());
  }

  @Test
  public void testSpilling() throws Exception {
    HiveConf conf = new HiveConf();
    HiveKVResultCache cache = new HiveKVResultCache(conf);

    final int recordCount = HiveKVResultCache.IN_MEMORY_CACHE_SIZE * 3;

    // Test using the same cache where first n rows are inserted then cache is cleared.
    // Next reuse the same cache and insert another m rows and verify the cache stores correctly.
    // This simulates reusing the same cache over and over again.
    testSpillingHelper(cache, recordCount);
    testSpillingHelper(cache, 1);
    testSpillingHelper(cache, recordCount);
  }

  /** Helper method which inserts numRecords and retrieves them from cache and verifies */
  private void testSpillingHelper(HiveKVResultCache cache, int numRecords) {
    for(int i=0; i<numRecords; i++) {
      String key = "key_" + i;
      String value = "value_" + i;
      cache.add(new BytesWritable(key.getBytes()), new BytesWritable(value.getBytes()));
    }

    int recordsSeen = 0;
    while(cache.hasNext()) {
      String key = "key_" + recordsSeen;
      String value = "value_" + recordsSeen;

      Tuple2<BytesWritable, BytesWritable> row = cache.next();
      assertTrue("Unexpected key at position: " + recordsSeen,
          new String(row._1().getBytes()).equals(key));
      assertTrue("Unexpected value at position: " + recordsSeen,
          new String(row._2().getBytes()).equals(value));

      recordsSeen++;
    }

    assertTrue("Retrieved record count doesn't match inserted record count",
        numRecords == recordsSeen);

    cache.clear();
  }
}