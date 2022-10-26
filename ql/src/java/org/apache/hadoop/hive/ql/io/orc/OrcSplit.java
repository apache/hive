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

package org.apache.hadoop.hive.ql.io.orc;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.AcidInputFormat;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.ColumnarSplit;
import org.apache.hadoop.hive.ql.io.LlapAwareSplit;
import org.apache.hadoop.hive.ql.io.SyntheticFileId;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.orc.OrcProto;
import org.apache.orc.impl.OrcTail;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OrcFileSplit. Holds file meta info
 *
 */
public class OrcSplit extends FileSplit implements ColumnarSplit, LlapAwareSplit {
  private static final Logger LOG = LoggerFactory.getLogger(OrcSplit.class);
  private OrcTail orcTail;
  private boolean hasFooter;
  /**
   * This means {@link AcidUtils.AcidBaseFileType#ORIGINAL_BASE}
   */
  private boolean isOriginal;
  private boolean hasBase;
  //partition root
  private Path rootDir;
  private final List<AcidInputFormat.DeltaMetaData> deltas = new ArrayList<>();
  private long projColsUncompressedSize;
  private transient Object fileKey;
  private long fileLen;
  private transient long writeId = 0;
  private transient int bucketId = 0;
  private transient int stmtId = 0;

  /**
   * This contains the synthetic ROW__ID offset and bucket properties for original file splits in an ACID table.
   */
  private OffsetAndBucketProperty syntheticAcidProps;

  static final int HAS_SYNTHETIC_ACID_PROPS_FLAG = 32;
  static final int HAS_SYNTHETIC_FILEID_FLAG = 16;
  static final int HAS_LONG_FILEID_FLAG = 8;
  static final int BASE_FLAG = 4;
  static final int ORIGINAL_FLAG = 2;
  static final int FOOTER_FLAG = 1;

  protected OrcSplit() {
    //The FileSplit() constructor in hadoop 0.20 and 1.x is package private so can't use it.
    //This constructor is used to create the object and then call readFields()
    // so just pass nulls to this super constructor.
    super(null, 0, 0, (String[]) null);
  }

  public OrcSplit(Path path, Object fileId, long offset, long length, String[] hosts,
      OrcTail orcTail, boolean isOriginal, boolean hasBase,
      List<AcidInputFormat.DeltaMetaData> deltas, long projectedDataSize, long fileLen, Path rootDir,
      OffsetAndBucketProperty syntheticAcidProps) {
    super(path, offset, length, hosts);
    // For HDFS, we could avoid serializing file ID and just replace the path with inode-based
    // path. However, that breaks bunch of stuff because Hive later looks up things by split path.
    this.fileKey = fileId;
    this.orcTail = orcTail;
    hasFooter = this.orcTail != null;
    this.isOriginal = isOriginal;
    this.hasBase = hasBase;
    this.rootDir = rootDir;
    int bucketId = AcidUtils.parseBucketId(path);
    long minWriteId = !deltas.isEmpty() ?
            AcidUtils.parseBaseOrDeltaBucketFilename(path, null).getMinimumWriteId() : -1;
    this.deltas.addAll(
            deltas.stream()
            // filtering out delete deltas with transactions happened before the transactions of the split
            .filter(delta -> delta.getMaxWriteId() >= minWriteId)
            .flatMap(delta -> filterDeltasByBucketId(delta, bucketId))
            .collect(Collectors.toList()));
    this.projColsUncompressedSize = projectedDataSize <= 0 ? length : projectedDataSize;
    // setting file length to Long.MAX_VALUE will let orc reader read file length from file system
    this.fileLen = fileLen <= 0 ? Long.MAX_VALUE : fileLen;
    this.syntheticAcidProps = syntheticAcidProps;
  }

  /**
   * For every split we only want to keep the delete deltas, that contains files for that bucket.
   * If we filter out files, we might need to filter out statementIds from multistatement transactions.
   * @param dmd
   * @param bucketId
   * @return
   */
  private Stream<AcidInputFormat.DeltaMetaData> filterDeltasByBucketId(AcidInputFormat.DeltaMetaData dmd, int bucketId) {
      Map<Integer, AcidInputFormat.DeltaFileMetaData> bucketFilesbyStmtId =
          dmd.getDeltaFiles().stream().filter(deltaFileMetaData -> deltaFileMetaData.getBucketId() == bucketId)
              .collect(Collectors.toMap(AcidInputFormat.DeltaFileMetaData::getStmtId, Function.identity()));
      if (bucketFilesbyStmtId.isEmpty()) {
        return Stream.empty();
      }
      // Keep only the relevant stmtIds
      List<Integer> stmtIds = dmd.getStmtIds().stream()
          .filter(stmtId -> bucketFilesbyStmtId.containsKey(stmtId))
          .collect(Collectors.toList());
      // For a small optimization clear the stmtIds from the files, if the delta is single statement delta
      List<AcidInputFormat.DeltaFileMetaData> bucketFiles = bucketFilesbyStmtId.values().stream()
          .map(file -> new AcidInputFormat.DeltaFileMetaData(file.getModTime(), file.getLength(), file.getAttemptId(),
              file.getFileId(), stmtIds.size() > 1 ? file.getStmtId() : null, bucketId))
          .collect(Collectors.toList());

      return Stream.of(new AcidInputFormat.DeltaMetaData(dmd.getMinWriteId(), dmd.getMaxWriteId(), stmtIds,
          dmd.getVisibilityTxnId(), bucketFiles));
  }

  @Override
  public void write(DataOutput out) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(bos);
    // serialize path, offset, length using FileSplit
    super.write(dos);
    int required = bos.size();

    // write addition payload required for orc
    writeAdditionalPayload(dos);
    int additional = bos.size() - required;

    out.write(bos.toByteArray());
    if (LOG.isTraceEnabled()) {
      LOG.trace("Writing additional {} bytes to OrcSplit as payload. Required {} bytes.",
          additional, required);
    }
  }

  private void writeAdditionalPayload(final DataOutputStream out) throws IOException {
    boolean isFileIdLong = fileKey instanceof Long, isFileIdWritable = fileKey instanceof Writable;
    int flags = (hasBase ? BASE_FLAG : 0) |
        (isOriginal ? ORIGINAL_FLAG : 0) |
        (hasFooter ? FOOTER_FLAG : 0) |
        (isFileIdLong ? HAS_LONG_FILEID_FLAG : 0) |
        (isFileIdWritable ? HAS_SYNTHETIC_FILEID_FLAG : 0) |
        (syntheticAcidProps != null? HAS_SYNTHETIC_ACID_PROPS_FLAG : 0);
    out.writeByte(flags);
    out.writeInt(deltas.size());
    for(AcidInputFormat.DeltaMetaData delta: deltas) {
      delta.write(out);
    }
    if (hasFooter) {
      OrcProto.FileTail fileTail = orcTail.getMinimalFileTail();
      byte[] tailBuffer = fileTail.toByteArray();
      int tailLen = tailBuffer.length;
      WritableUtils.writeVInt(out, tailLen);
      out.write(tailBuffer);
    }
    if (isFileIdLong) {
      out.writeLong(((Long)fileKey).longValue());
    } else if (isFileIdWritable) {
      ((Writable)fileKey).write(out);
    }
    out.writeLong(fileLen);
    out.writeUTF(rootDir.toString());
    if (syntheticAcidProps != null) {
      out.writeLong(syntheticAcidProps.rowIdOffset);
      out.writeInt(syntheticAcidProps.bucketProperty);
      out.writeLong(syntheticAcidProps.syntheticWriteId);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    //deserialize path, offset, length using FileSplit
    super.readFields(in);

    byte flags = in.readByte();
    hasFooter = (FOOTER_FLAG & flags) != 0;
    isOriginal = (ORIGINAL_FLAG & flags) != 0;
    hasBase = (BASE_FLAG & flags) != 0;
    boolean hasLongFileId = (HAS_LONG_FILEID_FLAG & flags) != 0,
        hasWritableFileId = (HAS_SYNTHETIC_FILEID_FLAG & flags) != 0,
        hasSyntheticProps = (HAS_SYNTHETIC_ACID_PROPS_FLAG & flags) != 0;
    if (hasLongFileId && hasWritableFileId) {
      throw new IOException("Invalid split - both file ID types present");
    }

    deltas.clear();
    int numDeltas = in.readInt();
    for(int i=0; i < numDeltas; i++) {
      AcidInputFormat.DeltaMetaData dmd = new AcidInputFormat.DeltaMetaData();
      dmd.readFields(in);
      deltas.add(dmd);
    }
    if (hasFooter) {
      int tailLen = WritableUtils.readVInt(in);
      byte[] tailBuffer = new byte[tailLen];
      in.readFully(tailBuffer);
      OrcProto.FileTail fileTail = OrcProto.FileTail.parseFrom(tailBuffer);
      orcTail = new OrcTail(fileTail, null);
    }
    if (hasLongFileId) {
      fileKey = in.readLong();
    } else if (hasWritableFileId) {
      SyntheticFileId fileId = new SyntheticFileId();
      fileId.readFields(in);
      this.fileKey = fileId;
    }
    fileLen = in.readLong();
    rootDir = new Path(in.readUTF());

    if (hasSyntheticProps) {
      long rowId = in.readLong();
      int bucket = in.readInt();
      long writeId = in.readLong();

      syntheticAcidProps = new OffsetAndBucketProperty(rowId, bucket, writeId);
    }
  }

  public OrcTail getOrcTail() {
    return orcTail;
  }

  public boolean hasFooter() {
    return hasFooter;
  }

  /**
   * @return {@code true} if file schema doesn't have Acid metadata columns
   * Such file may be in a delta_x_y/ or base_x due to being added via
   * "load data" command.  It could be at partition|table root due to table having
   * been converted from non-acid to acid table.  It could even be something like
   * "warehouse/t/HIVE_UNION_SUBDIR_15/000000_0" if it was written by an
   * "insert into t select ... from A union all select ... from B"
   */
  public boolean isOriginal() {
    return isOriginal;
  }

  public boolean hasBase() {
    return hasBase;
  }

  public Path getRootDir() {
    return rootDir;
  }
  public List<AcidInputFormat.DeltaMetaData> getDeltas() {
    return deltas;
  }

  public long getFileLength() {
    return fileLen;
  }

  /**
   * If this method returns true, then for sure it is ACID.
   * However, if it returns false.. it could be ACID or non-ACID.
   * @return
   */
  public boolean isAcid() {
    return hasBase || deltas.size() > 0;
  }

  public long getProjectedColumnsUncompressedSize() {
    return projColsUncompressedSize;
  }

  public Object getFileKey() {
    return fileKey;
  }

  public OffsetAndBucketProperty getSyntheticAcidProps() {
    return syntheticAcidProps;
  }

  @Override
  public long getColumnarProjectionSize() {
    return projColsUncompressedSize;
  }

  @Override
  public boolean canUseLlapIo(Configuration conf) {
    if (AcidUtils.isFullAcidScan(conf)) {
      if (HiveConf.getBoolVar(conf, ConfVars.LLAP_IO_ACID_ENABLED)
              && Utilities.getIsVectorized(conf)) {
        boolean hasDeleteDelta = deltas != null && !deltas.isEmpty();
        return VectorizedOrcAcidRowBatchReader.canUseLlapIoForAcid(this, hasDeleteDelta, conf);
      } else {
        LOG.info("Skipping Llap IO based on the following: [vectorized={}, hive.llap.io.acid={}] for {}",
            Utilities.getIsVectorized(conf), HiveConf.getBoolVar(conf, ConfVars.LLAP_IO_ACID_ENABLED), this);
        return false;
      }
    } else {
      return true;
    }
  }

  /**
   * Used for generating synthetic ROW__IDs for reading "original" files.
   */
  public static final class OffsetAndBucketProperty {
    private final long rowIdOffset;
    private final int bucketProperty;
    private final long syntheticWriteId;
    OffsetAndBucketProperty(long rowIdOffset, int bucketProperty, long syntheticWriteId) {
      this.rowIdOffset = rowIdOffset;
      this.bucketProperty = bucketProperty;
      this.syntheticWriteId = syntheticWriteId;
    }

    public long getRowIdOffset() {
      return rowIdOffset;
    }

    public int getBucketProperty() {
      return bucketProperty;
    }

    public long getSyntheticWriteId() {
      return syntheticWriteId;
    }
  }
  
  /**
   * Note: this is the write id as seen in the file name that contains this split
   * For files that have min/max writeId, this is the starting one.  
   * @return
   */
  public long getWriteId() {
    return writeId;
  }

  public int getStatementId() {
    return stmtId;
  }

  /**
   * Note: this is the bucket number as seen in the file name that contains this split.
   * Hive 3.0 encodes a bunch of info in the Acid schema's bucketId attribute.
   * See: {@link org.apache.hadoop.hive.ql.io.BucketCodec#V1} for details.
   * @return
   */
  public int getBucketId() {
    return bucketId;
  }

  public void parse(Configuration conf) throws IOException {
    parse(conf, rootDir);
  }

  public void parse(Configuration conf, Path rootPath) throws IOException {
    OrcRawRecordMerger.TransactionMetaData tmd =
        OrcRawRecordMerger.TransactionMetaData.findWriteIDForSynthetcRowIDs(getPath(), rootPath, conf);
    writeId = tmd.syntheticWriteId;
    stmtId = tmd.statementId;
    bucketId = AcidUtils.parseBucketId(getPath());
  }

  @Override
  public String toString() {
    return "OrcSplit [" + getPath() + ", start=" + getStart() + ", length=" + getLength()
        + ", isOriginal=" + isOriginal + ", fileLength=" + fileLen + ", hasFooter=" + hasFooter +
        ", hasBase=" + hasBase + ", deltas=" + (deltas == null ? 0 : deltas.size()) + "]";
  }
}
