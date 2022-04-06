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

package org.apache.hadoop.hive.ql.io;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.orc.OrcRawRecordMerger;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.shims.HadoopShims;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import javax.annotation.Nullable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
 * at the top level to having base and delta directories. Each write operation in a table
 * will be assigned a sequential table write id and each read operation
 * will request the list of valid transactions/write ids.
 * <ul>
 *   <li>Old format -
 *     <pre>
 *        $partition/$bucket
 *     </pre></li>
 *   <li>New format -
 *     <pre>
 *        $partition/base_$wid/$bucket
 *                   delta_$wid_$wid_$stid/$bucket
 *     </pre></li>
 * </ul>
 * <p>
 * With each new write operation a new delta directory is created with events
 * that correspond to inserted, updated, or deleted rows. Each of the files is
 * stored sorted by the original write id (ascending), bucket (ascending),
 * row id (ascending), and current write id (descending). Thus the files
 * can be merged by advancing through the files in parallel.
 * The stid is unique id (within the transaction) of the statement that created
 * this delta file.
 * <p>
 * The base files include all transactions from the beginning of time
 * (write id 0) to the write id in the directory name. Delta
 * directories include transactions (inclusive) between the two write ids.
 * <p>
 * Because read operations get the list of valid transactions/write ids when they start,
 * all reads are performed on that snapshot, regardless of any transactions that
 * are committed afterwards.
 * <p>
 * The base and the delta directories have the write ids so that major
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

  final class DeltaMetaData implements Writable {
    private long minWriteId;
    private long maxWriteId;
    private List<Integer> stmtIds;
    /**
     * {@link AcidUtils#?}
     */
    private long visibilityTxnId;

    private List<DeltaFileMetaData> deltaFiles;

    public DeltaMetaData() {
      this(0, 0, new ArrayList<>(), 0, new ArrayList<>());
    }

    /**
     * @param minWriteId min writeId of the delta directory
     * @param maxWriteId max writeId of the delta directory
     * @param stmtIds delta dir suffixes when a single txn writes &gt; 1 delta in the same partition
     * @param visibilityTxnId maybe 0, if the dir name didn't have it.  txnid:0 is always visible
     * @param deltaFiles bucketFiles in the directory
     */
    public DeltaMetaData(long minWriteId, long maxWriteId, List<Integer> stmtIds, long visibilityTxnId,
        List<DeltaFileMetaData> deltaFiles) {
      this.minWriteId = minWriteId;
      this.maxWriteId = maxWriteId;
      if (stmtIds == null) {
        throw new IllegalArgumentException("stmtIds == null");
      }
      this.stmtIds = stmtIds;
      this.visibilityTxnId = visibilityTxnId;
      this.deltaFiles = ObjectUtils.defaultIfNull(deltaFiles, new ArrayList<>());
    }

    public long getMinWriteId() {
      return minWriteId;
    }

    public long getMaxWriteId() {
      return maxWriteId;
    }

    public List<Integer> getStmtIds() {
      return stmtIds;
    }

    public long getVisibilityTxnId() {
      return visibilityTxnId;
    }

    public List<DeltaFileMetaData> getDeltaFiles() {
      return deltaFiles;
    }

    public List<DeltaFileMetaData> getDeltaFilesForStmtId(final Integer stmtId) {
      if (stmtIds.size() <= 1 || stmtId == null) {
        // If it is not a multistatement delta, we do not store the stmtId in the file list
        return deltaFiles;
      } else {
        return deltaFiles.stream().filter(df -> stmtId.equals(df.getStmtId())).collect(Collectors.toList());
      }
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeLong(minWriteId);
      out.writeLong(maxWriteId);
      out.writeInt(stmtIds.size());
      for (Integer id : stmtIds) {
        out.writeInt(id);
      }
      out.writeLong(visibilityTxnId);
      out.writeInt(deltaFiles.size());
      for (DeltaFileMetaData fileMeta : deltaFiles) {
        fileMeta.write(out);
      }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      minWriteId = in.readLong();
      maxWriteId = in.readLong();
      stmtIds.clear();
      int numStatements = in.readInt();
      for (int i = 0; i < numStatements; i++) {
        stmtIds.add(in.readInt());
      }
      visibilityTxnId = in.readLong();

      deltaFiles.clear();
      int numFiles = in.readInt();
      for (int i = 0; i < numFiles; i++) {
        DeltaFileMetaData file = new DeltaFileMetaData();
        file.readFields(in);
        deltaFiles.add(file);
      }
    }

    private String getName() {
      assert stmtIds.isEmpty() : "use getName(int)";
      return AcidUtils.addVisibilitySuffix(AcidUtils.deleteDeltaSubdir(minWriteId, maxWriteId), visibilityTxnId);
    }

    private String getName(int stmtId) {
      assert !stmtIds.isEmpty() : "use getName()";
      return AcidUtils.addVisibilitySuffix(AcidUtils
          .deleteDeltaSubdir(minWriteId, maxWriteId, stmtId), visibilityTxnId);
    }

    public List<Pair<Path, Integer>> getPaths(Path root) {
      if (stmtIds.isEmpty()) {
        return Collections.singletonList(new ImmutablePair<>(new Path(root, getName()), null));
      } else {
        // To support multistatement transactions we may have multiple directories corresponding to one DeltaMetaData
        return getStmtIds().stream()
            .map(stmtId -> new ImmutablePair<>(new Path(root, getName(stmtId)), stmtId)).collect(Collectors.toList());
      }
    }

    @Override
    public String toString() {
      return "Delta(?," + minWriteId + "," + maxWriteId + "," + stmtIds + "," + visibilityTxnId + ")";
    }
  }

  final class DeltaFileMetaData implements Writable {
    private static final int HAS_LONG_FILEID_FLAG = 1;
    private static final int HAS_ATTEMPTID_FLAG = 2;
    private static final int HAS_STMTID_FLAG = 4;

    private long modTime;
    private long length;
    // Optional
    private Integer attemptId;
    // Optional
    private Long fileId;
    // Optional, if the deltaMeta contains multiple stmtIds, it will contain this files parent's stmtId
    private Integer stmtId;

    // Not serialized
    private int bucketId;

    public DeltaFileMetaData() {
    }

    public DeltaFileMetaData(HadoopShims.HdfsFileStatusWithId fileStatus, Integer stmtId, int bucketId) {
      modTime = fileStatus.getFileStatus().getModificationTime();
      length = fileStatus.getFileStatus().getLen();
      attemptId = AcidUtils.parseAttemptId(fileStatus.getFileStatus().getPath());
      fileId = fileStatus.getFileId();
      this.stmtId = stmtId;
      this.bucketId = bucketId;
    }

    public DeltaFileMetaData(long modTime, long length, @Nullable Integer attemptId, @Nullable Long fileId,
        @Nullable Integer stmtId, int bucketId) {
      this.modTime = modTime;
      this.length = length;
      this.attemptId = attemptId;
      this.fileId = fileId;
      this.stmtId = stmtId;
      this.bucketId = bucketId;
    }

    public Integer getStmtId() {
      return stmtId;
    }

    public long getModTime() {
      return modTime;
    }

    public long getLength() {
      return length;
    }

    public Integer getAttemptId() {
      return attemptId;
    }

    public Long getFileId() {
      return fileId;
    }

    public int getBucketId() {
      return bucketId;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      int flags = (fileId != null ? HAS_LONG_FILEID_FLAG : 0) |
          (attemptId != null ? HAS_ATTEMPTID_FLAG : 0) |
          (stmtId != null ? HAS_STMTID_FLAG : 0);
      out.writeByte(flags);
      out.writeLong(modTime);
      out.writeLong(length);
      out.writeInt(bucketId);
      if (attemptId != null) {
        out.writeInt(attemptId);
      }
      if (fileId != null) {
        out.writeLong(fileId);
      }
      if (stmtId != null) {
        out.writeInt(stmtId);
      }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      byte flags = in.readByte();
      boolean hasLongFileId = (HAS_LONG_FILEID_FLAG & flags) != 0,
          hasAttemptId = (HAS_ATTEMPTID_FLAG & flags) != 0,
          hasStmtId = (HAS_STMTID_FLAG & flags) != 0;
      modTime = in.readLong();
      length = in.readLong();
      bucketId = in.readInt();
      if (hasAttemptId) {
        attemptId = in.readInt();
      }
      if (hasLongFileId) {
        fileId = in.readLong();
      }
      if (hasStmtId) {
        stmtId = in.readInt();
      }
    }

    public Object getFileId(Path deltaFile, int bucketId, Configuration conf) {
      boolean forceSynthetic = !HiveConf.getBoolVar(conf, HiveConf.ConfVars.LLAP_IO_USE_FILEID_PATH);
      if (fileId != null && !forceSynthetic) {
        return fileId;
      }
      // Calculate the synthetic fileid
      return new SyntheticFileId(deltaFile, length, modTime);
    }

    public Path getPath(Path deltaDirectory, int bucketId) {
      return AcidUtils.createBucketFile(deltaDirectory, bucketId, attemptId);
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
      extends RecordReader<OrcRawRecordMerger.ReaderKey, V> {
    public ObjectInspector getObjectInspector();
  }

  /**
   * Get a record reader that provides the user-facing view of the data after
   * it has been merged together. The key provides information about the
   * record's identifier (write id, bucket, record id).
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
   * @param validWriteIdList the list of valid write ids to use
   * @param baseDirectory the base directory to read or the root directory for
   *                      old style files
   * @param deltaDirectory a list of delta files to include in the merge
   * @return a record reader
   * @throws IOException
   */
   RawReader<VALUE> getRawReader(Configuration conf,
                             boolean collapseEvents,
                             int bucket,
                             ValidWriteIdList validWriteIdList,
                             Path baseDirectory,
                             Path[] deltaDirectory,
                             Map<String, Integer> deltasToAttemptId
                             ) throws IOException;

  /**
   * RecordReader returned by AcidInputFormat working in row-at-a-time mode should AcidRecordReader.
   */
  public interface AcidRecordReader<K,V> extends RecordReader<K,V> {
    OrcRawRecordMerger.ReaderKey getRecordIdentifier();
  }
}
