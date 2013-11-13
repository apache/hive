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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hcatalog.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.hbase.ResultWritable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hcatalog.common.HCatUtil;
import org.apache.hcatalog.hbase.snapshot.FamilyRevision;
import org.apache.hcatalog.hbase.snapshot.RevisionManager;
import org.apache.hcatalog.hbase.snapshot.TableSnapshot;
import org.apache.hcatalog.mapreduce.InputJobInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Class HbaseSnapshotRecordReader implements logic for filtering records
 * based on snapshot.
 */
class HbaseSnapshotRecordReader implements RecordReader<ImmutableBytesWritable, ResultWritable> {

  static final Logger LOG = LoggerFactory.getLogger(HbaseSnapshotRecordReader.class);
  private final InputJobInfo inpJobInfo;
  private final Configuration conf;
  private final int maxRevisions = 1;
  private ResultScanner scanner;
  private Scan scan;
  private HTable htable;
  private TableSnapshot snapshot;
  private Iterator<Result> resultItr;
  private Set<Long> allAbortedTransactions;

  HbaseSnapshotRecordReader(InputJobInfo inputJobInfo, Configuration conf) throws IOException {
    this.inpJobInfo = inputJobInfo;
    this.conf = conf;
    String snapshotString = conf.get(HBaseConstants.PROPERTY_TABLE_SNAPSHOT_KEY);
    HCatTableSnapshot hcatSnapshot = (HCatTableSnapshot) HCatUtil
      .deserialize(snapshotString);
    this.snapshot = HBaseRevisionManagerUtil.convertSnapshot(hcatSnapshot,
      inpJobInfo.getTableInfo());
  }

  public void init() throws IOException {
    restart(scan.getStartRow());
  }

  public void restart(byte[] firstRow) throws IOException {
    allAbortedTransactions = getAbortedTransactions(Bytes.toString(htable.getTableName()), scan);
    long maxValidRevision = getMaximumRevision(scan, snapshot);
    while (allAbortedTransactions.contains(maxValidRevision)) {
      maxValidRevision--;
    }
    Scan newScan = new Scan(scan);
    newScan.setStartRow(firstRow);
    //TODO: See if filters in 0.92 can be used to optimize the scan
    //TODO: Consider create a custom snapshot filter
    //TODO: Make min revision a constant in RM
    newScan.setTimeRange(0, maxValidRevision + 1);
    newScan.setMaxVersions();
    this.scanner = this.htable.getScanner(newScan);
    resultItr = this.scanner.iterator();
  }

  private Set<Long> getAbortedTransactions(String tableName, Scan scan) throws IOException {
    Set<Long> abortedTransactions = new HashSet<Long>();
    RevisionManager rm = null;
    try {
      rm = HBaseRevisionManagerUtil.getOpenedRevisionManager(conf);
      byte[][] families = scan.getFamilies();
      for (byte[] familyKey : families) {
        String family = Bytes.toString(familyKey);
        List<FamilyRevision> abortedWriteTransactions = rm.getAbortedWriteTransactions(
          tableName, family);
        if (abortedWriteTransactions != null) {
          for (FamilyRevision revision : abortedWriteTransactions) {
            abortedTransactions.add(revision.getRevision());
          }
        }
      }
      return abortedTransactions;
    } finally {
      HBaseRevisionManagerUtil.closeRevisionManagerQuietly(rm);
    }
  }

  private long getMaximumRevision(Scan scan, TableSnapshot snapshot) {
    long maxRevision = 0;
    byte[][] families = scan.getFamilies();
    for (byte[] familyKey : families) {
      String family = Bytes.toString(familyKey);
      long revision = snapshot.getRevision(family);
      if (revision > maxRevision)
        maxRevision = revision;
    }
    return maxRevision;
  }

  /*
   * @param htable The HTable ( of HBase) to use for the record reader.
   *
   */
  public void setHTable(HTable htable) {
    this.htable = htable;
  }

  /*
   * @param scan The scan to be used for reading records.
   *
   */
  public void setScan(Scan scan) {
    this.scan = scan;
  }

  @Override
  public ImmutableBytesWritable createKey() {
    return new ImmutableBytesWritable();
  }

  @Override
  public ResultWritable createValue() {
    return new ResultWritable();
  }

  @Override
  public long getPos() {
    // This should be the ordinal tuple in the range;
    // not clear how to calculate...
    return 0;
  }

  @Override
  public float getProgress() throws IOException {
    // Depends on the total number of tuples
    return 0;
  }

  @Override
  public boolean next(ImmutableBytesWritable key, ResultWritable value) throws IOException {
    if (this.resultItr == null) {
      LOG.warn("The HBase result iterator is found null. It is possible"
        + " that the record reader has already been closed.");
    } else {
      while (resultItr.hasNext()) {
        Result temp = resultItr.next();
        Result hbaseRow = prepareResult(temp.list());
        if (hbaseRow != null) {
          // Update key and value. Currently no way to avoid serialization/de-serialization
          // as no setters are available.
          key.set(hbaseRow.getRow());
          value.setResult(hbaseRow);
          return true;
        }

      }
    }
    return false;
  }

  private Result prepareResult(List<KeyValue> keyvalues) {

    List<KeyValue> finalKeyVals = new ArrayList<KeyValue>();
    Map<String, List<KeyValue>> qualValMap = new HashMap<String, List<KeyValue>>();
    for (KeyValue kv : keyvalues) {
      byte[] cf = kv.getFamily();
      byte[] qualifier = kv.getQualifier();
      String key = Bytes.toString(cf) + ":" + Bytes.toString(qualifier);
      List<KeyValue> kvs;
      if (qualValMap.containsKey(key)) {
        kvs = qualValMap.get(key);
      } else {
        kvs = new ArrayList<KeyValue>();
      }

      String family = Bytes.toString(kv.getFamily());
      //Ignore aborted transactions
      if (allAbortedTransactions.contains(kv.getTimestamp())) {
        continue;
      }

      long desiredTS = snapshot.getRevision(family);
      if (kv.getTimestamp() <= desiredTS) {
        kvs.add(kv);
      }
      qualValMap.put(key, kvs);
    }

    Set<String> keys = qualValMap.keySet();
    for (String cf : keys) {
      List<KeyValue> kvs = qualValMap.get(cf);
      if (maxRevisions <= kvs.size()) {
        for (int i = 0; i < maxRevisions; i++) {
          finalKeyVals.add(kvs.get(i));
        }
      } else {
        finalKeyVals.addAll(kvs);
      }
    }

    if (finalKeyVals.size() == 0) {
      return null;
    } else {
      KeyValue[] kvArray = new KeyValue[finalKeyVals.size()];
      finalKeyVals.toArray(kvArray);
      return new Result(kvArray);
    }
  }

  /*
   * @see org.apache.hadoop.hbase.mapred.TableRecordReader#close()
   */
  @Override
  public void close() {
    this.resultItr = null;
    this.scanner.close();
  }

}
