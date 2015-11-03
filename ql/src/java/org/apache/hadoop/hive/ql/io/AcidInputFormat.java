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

package org.apache.hadoop.hive.ql.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * The interface required for input formats that what to support ACID
 * transactions.
 * <p>
 * The goal is to provide ACID transactions to Hive. There are
 * several primary use cases:
 * <ul>
 *   <li>Streaming ingest- Allow Flume or Storm to stream data into Hive
 *   tables with relatively low latency (~30 seconds).</li>
 *   <li>Dimension table update- Allow updates of dimension tables without
 *   overwriting the entire partition (or table) using standard SQL syntax.</li>
 *   <li>Fact table inserts- Insert data into fact tables at granularity other
 *   than entire partitions using standard SQL syntax.</li>
 *   <li>Fact table update- Update large fact tables to correct data that
 *   was previously loaded.</li>
 * </ul>
 * It is important to support batch updates and maintain read consistency within
 * a query. A non-goal is to support many simultaneous updates or to replace
 * online transactions systems.
 * <p>
 * The design changes the layout of data within a partition from being in files
 * at the top level to having base and delta directories. Each write operation
 * will be assigned a sequential global transaction id and each read operation
 * will request the list of valid transaction ids.
 * <ul>
 *   <li>Old format -
 *     <pre>
 *        $partition/$bucket
 *     </pre></li>
 *   <li>New format -
 *     <pre>
 *        $partition/base_$tid/$bucket
 *                   delta_$tid_$tid_$stid/$bucket
 *     </pre></li>
 * </ul>
 * <p>
 * With each new write operation a new delta directory is created with events
 * that correspond to inserted, updated, or deleted rows. Each of the files is
 * stored sorted by the original transaction id (ascending), bucket (ascending),
 * row id (ascending), and current transaction id (descending). Thus the files
 * can be merged by advancing through the files in parallel.
 * The stid is unique id (within the transaction) of the statement that created
 * this delta file.
 * <p>
 * The base files include all transactions from the beginning of time
 * (transaction id 0) to the transaction in the directory name. Delta
 * directories include transactions (inclusive) between the two transaction ids.
 * <p>
 * Because read operations get the list of valid transactions when they start,
 * all reads are performed on that snapshot, regardless of any transactions that
 * are committed afterwards.
 * <p>
 * The base and the delta directories have the transaction ids so that major
 * (merge all deltas into the base) and minor (merge several deltas together)
 * compactions can happen while readers continue their processing.
 * <p>
 * To support transitions between non-ACID layouts to ACID layouts, the input
 * formats are expected to support both layouts and detect the correct one.
 * <p>
 *   A note on the KEY of this InputFormat.  
 *   For row-at-a-time processing, KEY can conveniently pass RowId into the operator
 *   pipeline.  For vectorized execution the KEY could perhaps represent a range in the batch.
 *   Since {@link org.apache.hadoop.hive.ql.io.orc.OrcInputFormat} is declared to return
 *   {@code NullWritable} key, {@link org.apache.hadoop.hive.ql.io.AcidInputFormat.AcidRecordReader} is defined
 *   to provide access to the RowId.  Other implementations of AcidInputFormat can use either
 *   mechanism.
 * </p>
 * 
 * @param <VALUE> The row type
 */
public interface AcidInputFormat<KEY extends WritableComparable, VALUE>
    extends InputFormat<KEY, VALUE>, InputFormatChecker {

  static final class DeltaMetaData implements Writable {
    private long minTxnId;
    private long maxTxnId;
    private List<Integer> stmtIds;
    
    public DeltaMetaData() {
      this(0,0,new ArrayList<Integer>());
    }
    DeltaMetaData(long minTxnId, long maxTxnId, List<Integer> stmtIds) {
      this.minTxnId = minTxnId;
      this.maxTxnId = maxTxnId;
      if (stmtIds == null) {
        throw new IllegalArgumentException("stmtIds == null");
      }
      this.stmtIds = stmtIds;
    }
    long getMinTxnId() {
      return minTxnId;
    }
    long getMaxTxnId() {
      return maxTxnId;
    }
    List<Integer> getStmtIds() {
      return stmtIds;
    }
    @Override
    public void write(DataOutput out) throws IOException {
      out.writeLong(minTxnId);
      out.writeLong(maxTxnId);
      out.writeInt(stmtIds.size());
      for(Integer id : stmtIds) {
        out.writeInt(id);
      }
    }
    @Override
    public void readFields(DataInput in) throws IOException {
      minTxnId = in.readLong();
      maxTxnId = in.readLong();
      stmtIds.clear();
      int numStatements = in.readInt();
      for(int i = 0; i < numStatements; i++) {
        stmtIds.add(in.readInt());
      }
    }
  }
  /**
   * Options for controlling the record readers.
   */
  public static class Options {
    private final Configuration conf;
    private Reporter reporter;

    /**
     * Supply the configuration to use when reading.
     * @param conf
     */
    public Options(Configuration conf) {
      this.conf = conf;
    }

    /**
     * Supply the reporter.
     * @param reporter the MapReduce reporter object
     * @return this
     */
    public Options reporter(Reporter reporter) {
      this.reporter = reporter;
      return this;
    }

    public Configuration getConfiguration() {
      return conf;
    }

    public Reporter getReporter() {
      return reporter;
    }
  }

  public static interface RowReader<V>
      extends RecordReader<RecordIdentifier, V> {
    public ObjectInspector getObjectInspector();
  }

  /**
   * Get a record reader that provides the user-facing view of the data after
   * it has been merged together. The key provides information about the
   * record's identifier (transaction, bucket, record id).
   * @param split the split to read
   * @param options the options to read with
   * @return a record reader
   * @throws IOException
   */
  public RowReader<VALUE> getReader(InputSplit split,
                                Options options) throws IOException;

  public static interface RawReader<V>
      extends RecordReader<RecordIdentifier, V> {
    public ObjectInspector getObjectInspector();

    public boolean isDelete(V value);
  }

  /**
   * Get a reader that returns the raw ACID events (insert, update, delete).
   * Should only be used by the compactor.
   * @param conf the configuration
   * @param collapseEvents should the ACID events be collapsed so that only
   *                       the last version of the row is kept.
   * @param bucket the bucket to read
   * @param validTxnList the list of valid transactions to use
   * @param baseDirectory the base directory to read or the root directory for
   *                      old style files
   * @param deltaDirectory a list of delta files to include in the merge
   * @return a record reader
   * @throws IOException
   */
   RawReader<VALUE> getRawReader(Configuration conf,
                             boolean collapseEvents,
                             int bucket,
                             ValidTxnList validTxnList,
                             Path baseDirectory,
                             Path[] deltaDirectory
                             ) throws IOException;

  /**
   * RecordReader returned by AcidInputFormat working in row-at-a-time mode should AcidRecordReader.
   */
  public interface AcidRecordReader<K,V> extends RecordReader<K,V> {
    RecordIdentifier getRecordIdentifier();
  }
}
