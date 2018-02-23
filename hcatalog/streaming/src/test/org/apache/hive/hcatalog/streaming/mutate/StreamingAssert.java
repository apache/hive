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

package org.apache.hive.hcatalog.streaming.mutate;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.io.AcidInputFormat.AcidRecordReader;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.AcidUtils.Directory;
import org.apache.hadoop.hive.ql.io.RecordIdentifier;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.thrift.TException;

public class StreamingAssert {

  public static class Factory {
    private IMetaStoreClient metaStoreClient;
    private final HiveConf conf;

    public Factory(IMetaStoreClient metaStoreClient, HiveConf conf) {
      this.metaStoreClient = metaStoreClient;
      this.conf = conf;
    }

    public StreamingAssert newStreamingAssert(Table table) throws Exception {
      return newStreamingAssert(table, Collections.<String> emptyList());
    }

    public StreamingAssert newStreamingAssert(Table table, List<String> partition) throws Exception {
      return new StreamingAssert(metaStoreClient, conf, table, partition);
    }
  }

  private Table table;
  private List<String> partition;
  private IMetaStoreClient metaStoreClient;
  private Directory dir;
  private ValidWriteIdList writeIds;
  private List<AcidUtils.ParsedDelta> currentDeltas;
  private long min;
  private long max;
  private Path partitionLocation;

  StreamingAssert(IMetaStoreClient metaStoreClient, HiveConf conf, Table table, List<String> partition)
      throws Exception {
    this.metaStoreClient = metaStoreClient;
    this.table = table;
    this.partition = partition;

    writeIds = metaStoreClient.getValidWriteIds(AcidUtils.getFullTableName(table.getDbName(), table.getTableName()));
    partitionLocation = getPartitionLocation();
    dir = AcidUtils.getAcidState(partitionLocation, conf, writeIds);
    assertEquals(0, dir.getObsolete().size());
    assertEquals(0, dir.getOriginalFiles().size());

    currentDeltas = dir.getCurrentDirectories();
    min = Long.MAX_VALUE;
    max = Long.MIN_VALUE;
    System.out.println("Files found: ");
    for (AcidUtils.ParsedDelta parsedDelta : currentDeltas) {
      System.out.println(parsedDelta.getPath().toString());
      max = Math.max(parsedDelta.getMaxWriteId(), max);
      min = Math.min(parsedDelta.getMinWriteId(), min);
    }
  }

  public void assertExpectedFileCount(int expectedFileCount) {
    assertEquals(expectedFileCount, currentDeltas.size());
  }

  public void assertNothingWritten() {
    assertExpectedFileCount(0);
  }

  public void assertMinTransactionId(long expectedMinTransactionId) {
    if (currentDeltas.isEmpty()) {
      throw new AssertionError("No data");
    }
    assertEquals(expectedMinTransactionId, min);
  }

  public void assertMaxTransactionId(long expectedMaxTransactionId) {
    if (currentDeltas.isEmpty()) {
      throw new AssertionError("No data");
    }
    assertEquals(expectedMaxTransactionId, max);
  }

  List<Record> readRecords() throws Exception {
    return readRecords(1);
  }

  /**
   * TODO: this would be more flexible doing a SQL select statement rather than using InputFormat directly
   * see {@link org.apache.hive.hcatalog.streaming.TestStreaming#checkDataWritten2(Path, long, long, int, String, String...)}
   * @param numSplitsExpected
   * @return
   * @throws Exception
   */
  List<Record> readRecords(int numSplitsExpected) throws Exception {
    if (currentDeltas.isEmpty()) {
      throw new AssertionError("No data");
    }
    InputFormat<NullWritable, OrcStruct> inputFormat = new OrcInputFormat();
    JobConf job = new JobConf();
    job.set("mapred.input.dir", partitionLocation.toString());
    job.set(hive_metastoreConstants.BUCKET_COUNT, Integer.toString(table.getSd().getNumBuckets()));
    job.set(IOConstants.SCHEMA_EVOLUTION_COLUMNS, "id,msg");
    job.set(IOConstants.SCHEMA_EVOLUTION_COLUMNS_TYPES, "bigint:string");
    AcidUtils.setAcidOperationalProperties(job, true, null);
    job.setBoolean(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL, true);
    job.set(ValidWriteIdList.VALID_WRITEIDS_KEY, writeIds.toString());
    InputSplit[] splits = inputFormat.getSplits(job, 1);
    assertEquals(numSplitsExpected, splits.length);


    List<Record> records = new ArrayList<>();
    for(InputSplit is : splits) {
      final AcidRecordReader<NullWritable, OrcStruct> recordReader = (AcidRecordReader<NullWritable, OrcStruct>) inputFormat
        .getRecordReader(is, job, Reporter.NULL);

      NullWritable key = recordReader.createKey();
      OrcStruct value = recordReader.createValue();

      while (recordReader.next(key, value)) {
        RecordIdentifier recordIdentifier = recordReader.getRecordIdentifier();
        Record record = new Record(new RecordIdentifier(recordIdentifier.getWriteId(),
          recordIdentifier.getBucketProperty(), recordIdentifier.getRowId()), value.toString());
        System.out.println(record);
        records.add(record);
      }
      recordReader.close();
    }
    return records;
  }

  private Path getPartitionLocation() throws NoSuchObjectException, MetaException, TException {
    Path partitionLocacation;
    if (partition.isEmpty()) {
      partitionLocacation = new Path(table.getSd().getLocation());
    } else {
      // TODO: calculate this instead. Just because we're writing to the location doesn't mean that it'll
      // always be wanted in the meta store right away.
      List<Partition> partitionEntries = metaStoreClient.listPartitions(table.getDbName(), table.getTableName(),
          partition, (short) 1);
      partitionLocacation = new Path(partitionEntries.get(0).getSd().getLocation());
    }
    return partitionLocacation;
  }

  public static class Record {
    private RecordIdentifier recordIdentifier;
    private String row;

    Record(RecordIdentifier recordIdentifier, String row) {
      this.recordIdentifier = recordIdentifier;
      this.row = row;
    }

    public RecordIdentifier getRecordIdentifier() {
      return recordIdentifier;
    }

    public String getRow() {
      return row;
    }

    @Override
    public String toString() {
      return "Record [recordIdentifier=" + recordIdentifier + ", row=" + row + "]";
    }

  }

}
