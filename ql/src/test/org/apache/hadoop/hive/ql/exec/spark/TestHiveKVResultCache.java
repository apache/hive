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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.io.BytesWritable;
import org.junit.Test;

import scala.Tuple2;

import com.clearspring.analytics.util.Preconditions;

@SuppressWarnings({"unchecked", "rawtypes"})
public class TestHiveKVResultCache {
  @Test
  public void testSimple() throws Exception {
    // Create KV result cache object, add one (k,v) pair and retrieve them.
    HiveKVResultCache cache = new HiveKVResultCache();

    HiveKey key = new HiveKey("key".getBytes(), "key".hashCode());
    BytesWritable value = new BytesWritable("value".getBytes());
    cache.add(key, value);

    assertTrue("KV result cache should have at least one element", cache.hasNext());

    Tuple2<HiveKey, BytesWritable> row = cache.next();
    assertTrue("Incorrect key", row._1().equals(key));
    assertTrue("Incorrect value", row._2().equals(value));

    assertTrue("Cache shouldn't have more records", !cache.hasNext());
  }

  @Test
  public void testSpilling() throws Exception {
    HiveKVResultCache cache = new HiveKVResultCache();

    final int recordCount = HiveKVResultCache.IN_MEMORY_NUM_ROWS * 3;

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
      cache.add(new HiveKey(key.getBytes(), key.hashCode()), new BytesWritable(value.getBytes()));
    }

    int recordsSeen = 0;
    while(cache.hasNext()) {
      String key = "key_" + recordsSeen;
      String value = "value_" + recordsSeen;

      Tuple2<HiveKey, BytesWritable> row = cache.next();
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

  @Test
  public void testResultList() throws Exception {
    scanAndVerify(10000, 0, 0, "a", "b");
    scanAndVerify(10000, 511, 0, "a", "b");
    scanAndVerify(10000, 511 * 2, 0, "a", "b");
    scanAndVerify(10000, 511, 10, "a", "b");
    scanAndVerify(10000, 511 * 2, 10, "a", "b");
    scanAndVerify(10000, 512, 0, "a", "b");
    scanAndVerify(10000, 512 * 2, 0, "a", "b");
    scanAndVerify(10000, 512, 3, "a", "b");
    scanAndVerify(10000, 512 * 6, 10, "a", "b");
    scanAndVerify(10000, 512 * 7, 5, "a", "b");
    scanAndVerify(10000, 512 * 9, 19, "a", "b");
    scanAndVerify(10000, 1, 0, "a", "b");
    scanAndVerify(10000, 1, 1, "a", "b");
  }

  private static void scanAndVerify(
      long rows, int threshold, int separate, String prefix1, String prefix2) {
    ArrayList<Tuple2<HiveKey, BytesWritable>> output =
      new ArrayList<Tuple2<HiveKey, BytesWritable>>((int)rows);
    scanResultList(rows, threshold, separate, output, prefix1, prefix2);
    assertEquals(rows, output.size());
    long  primaryRows = rows * (100 - separate) / 100;
    long separateRows = rows - primaryRows;
    HashSet<Long> primaryRowKeys = new HashSet<Long>();
    HashSet<Long> separateRowKeys = new HashSet<Long>();
    for (Tuple2<HiveKey, BytesWritable> item: output) {
      String key = bytesWritableToString(item._1);
      String value = bytesWritableToString(item._2);
      String prefix = key.substring(0, key.indexOf('_'));
      Long id = Long.valueOf(key.substring(5 + prefix.length()));
      if (prefix.equals(prefix1)) {
        assertTrue(id >= 0 && id < primaryRows);
        primaryRowKeys.add(id);
      } else {
        assertEquals(prefix2, prefix);
        assertTrue(id >= 0 && id < separateRows);
        separateRowKeys.add(id);
      }
      assertEquals(prefix + "_value_" + id, value);
    }
    assertEquals(separateRows, separateRowKeys.size());
    assertEquals(primaryRows, primaryRowKeys.size());
  }

  /**
   * Convert a BytesWritable to a string.
   * Don't use {@link BytesWritable#copyBytes()}
   * so as to be compatible with hadoop 1
   */
  private static String bytesWritableToString(BytesWritable bw) {
    int size = bw.getLength();
    byte[] bytes = new byte[size];
    System.arraycopy(bw.getBytes(), 0, bytes, 0, size);
    return new String(bytes);
  }

  private static class MyHiveFunctionResultList extends HiveBaseFunctionResultList {
    private static final long serialVersionUID = -1L;

    // Total rows to emit during the whole iteration,
    // excluding the rows emitted by the separate thread.
    private long primaryRows;
    // Batch of rows to emit per processNextRecord() call.
    private int thresholdRows;
    // Rows to be emitted with a separate thread per processNextRecord() call.
    private long separateRows;
    // Thread to generate the separate rows beside the normal thread.
    private Thread separateRowGenerator;

    // Counter for rows emitted
    private long rowsEmitted;
    private long separateRowsEmitted;

    // Prefix for primary row keys
    private String prefix1;
    // Prefix for separate row keys
    private String prefix2;

    // A queue to notify separateRowGenerator to generate the next batch of rows.
    private LinkedBlockingQueue<Boolean> queue;

    MyHiveFunctionResultList(Iterator inputIterator) {
      super(inputIterator);
    }

    void init(long rows, int threshold, int separate, String p1, String p2) {
      Preconditions.checkArgument((threshold > 0 || separate == 0)
        && separate < 100 && separate >= 0 && rows > 0);
      primaryRows = rows * (100 - separate) / 100;
      separateRows = rows - primaryRows;
      thresholdRows = threshold;
      prefix1 = p1;
      prefix2 = p2;
      if (separateRows > 0) {
        separateRowGenerator = new Thread(new Runnable() {
          @Override
          public void run() {
            try {
              long separateBatchSize = thresholdRows * separateRows / primaryRows;
              while (!queue.take().booleanValue()) {
                for (int i = 0; i < separateBatchSize; i++) {
                  collect(prefix2, separateRowsEmitted++);
                }
              }
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
            for (; separateRowsEmitted < separateRows;) {
              collect(prefix2, separateRowsEmitted++);
            }
          }
        });
        queue = new LinkedBlockingQueue<Boolean>();
        separateRowGenerator.start();
      }
    }

    public void collect(String prefix, long id) {
      String k = prefix + "_key_" + id;
      String v = prefix + "_value_" + id;
      HiveKey key = new HiveKey(k.getBytes(), k.hashCode());
      BytesWritable value = new BytesWritable(v.getBytes());
      try {
        collect(key, value);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    @Override
    protected void processNextRecord(Object inputRecord) throws IOException {
      for (int i = 0; i < thresholdRows; i++) {
        collect(prefix1, rowsEmitted++);
      }
      if (separateRowGenerator != null) {
        queue.add(Boolean.FALSE);
      }
    }

    @Override
    protected boolean processingDone() {
      return false;
    }

    @Override
    protected void closeRecordProcessor() {
      for (; rowsEmitted < primaryRows;) {
        collect(prefix1, rowsEmitted++);
      }
      if (separateRowGenerator != null) {
        queue.add(Boolean.TRUE);
        try {
          separateRowGenerator.join();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
  }

  private static long scanResultList(long rows, int threshold, int separate,
      List<Tuple2<HiveKey, BytesWritable>> output, String prefix1, String prefix2) {
    final long iteratorCount = threshold == 0 ? 1 : rows * (100 - separate) / 100 / threshold;
    MyHiveFunctionResultList resultList = new MyHiveFunctionResultList(new Iterator() {
      // Input record iterator, not used
      private int i = 0;
      @Override
      public boolean hasNext() {
        return i++ < iteratorCount;
      }

      @Override
      public Object next() {
        return Integer.valueOf(i);
      }

      @Override
      public void remove() {
      }
    });

    resultList.init(rows, threshold, separate, prefix1, prefix2);
    long startTime = System.currentTimeMillis();
    Iterator it = resultList.iterator();
    while (it.hasNext()) {
      Object item = it.next();
      if (output != null) {
        output.add((Tuple2<HiveKey, BytesWritable>)item);
      }
    }
    long endTime = System.currentTimeMillis();
    return endTime - startTime;
  }

  private static long[] scanResultList(long rows, int threshold, int extra) {
    // 1. Simulate emitting all records in closeRecordProcessor().
    long t1 = scanResultList(rows, 0, 0, null, "a", "b");

    // 2. Simulate emitting records in processNextRecord() with small memory usage limit.
    long t2 = scanResultList(rows, threshold, 0, null, "c", "d");

    // 3. Simulate emitting records in processNextRecord() with large memory usage limit.
    long t3 = scanResultList(rows, threshold * 10, 0, null, "e", "f");

    // 4. Same as 2. Also emit extra records from a separate thread.
    long t4 = scanResultList(rows, threshold, extra, null, "g", "h");

    // 5. Same as 3. Also emit extra records from a separate thread.
    long t5 = scanResultList(rows, threshold * 10, extra, null, "i", "j");

    return new long[] {t1, t2, t3, t4, t5};
  }

  public static void main(String[] args) throws Exception {
    long rows = 1000000; // total rows to generate
    int threshold = 512; // # of rows to cache at most
    int extra = 5; // percentile of extra rows to generate by a different thread

    if (args.length > 0) {
      rows = Long.parseLong(args[0]);
    }
    if (args.length > 1) {
      threshold = Integer.parseInt(args[1]);
    }
    if (args.length > 2) {
      extra = Integer.parseInt(args[2]);
    }

    // Warm up couple times
    for (int i = 0; i < 2; i++) {
      scanResultList(rows, threshold, extra);
    }

    int count = 5;
    long[] t = new long[count];
    // Run count times and get average
    for (int i = 0; i < count; i++) {
      long[] tmp = scanResultList(rows, threshold, extra);
      for (int k = 0; k < count; k++) {
        t[k] += tmp[k];
      }
    }
    for (int i = 0; i < count; i++) {
      t[i] /= count;
    }

    System.out.println(t[0] + "\t" + t[1] + "\t" + t[2]
      + "\t" + t[3] + "\t" + t[4]);
  }
}
