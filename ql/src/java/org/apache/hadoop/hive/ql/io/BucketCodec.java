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

/**
 * This class makes sense of {@link RecordIdentifier#getBucketProperty()}.  Up until ASF Hive 3.0 this
 * field was simply the bucket ID.  Since 3.0 it does bit packing to store several things:
 * top 3 bits - version describing the format (we can only have 8).
 * The rest is version specific - see below.
 */
public enum BucketCodec {
  /**
   * This is the "legacy" version.  The whole {@code bucket} value just has the bucket ID in it.
   * The numeric code for this version is 0. (Assumes bucket ID takes less than 29 bits... which
   * implies top 3 bits are 000 so data written before Hive 3.0 is readable with this scheme).
   */
  V0(0) {
    @Override
    public int decodeWriterId(int bucketProperty) {
      return bucketProperty;
    }
    @Override
    public int decodeStatementId(int bucketProperty) {
      return 0;
    }
    @Override
    public int encode(AcidOutputFormat.Options options) {
      return options.getBucketId();
    }
  },
  /**
   * Represents format of "bucket" property in Hive 3.0.
   * top 3 bits - version code.
   * next 1 bit - reserved for future
   * next 12 bits - the bucket ID
   * next 4 bits reserved for future
   * remaining 12 bits - the statement ID - 0-based numbering of all statements within a
   * transaction.  Each leg of a multi-insert statement gets a separate statement ID.
   * The reserved bits align it so that it easier to interpret it in Hex.
   * 
   * Constructs like Merge and Multi-Insert may have multiple tasks writing data that belongs to
   * the same physical bucket file.  For example, a Merge stmt with update and insert clauses,
   * (and split update enabled - should be the default in 3.0).  A task on behalf of insert may
   * be writing a row into bucket 0 and another task in the update branch may be writing an insert
   * event into bucket 0.  Each of these task are writing to different delta directory - distinguished
   * by statement ID.  By including both bucket ID and statement ID in {@link RecordIdentifier}
   * we ensure that {@link RecordIdentifier} is unique.
   * 
   * The intent is that sorting rows by {@link RecordIdentifier} groups rows in the same physical
   * bucket next to each other.
   * For any row created by a given version of Hive, top 3 bits are constant.  The next
   * most significant bits are the bucket ID, then the statement ID.  This ensures that
   * {@link org.apache.hadoop.hive.ql.optimizer.SortedDynPartitionOptimizer} works which is
   * designed so that each task only needs to keep 1 writer opened at a time.  It could be
   * configured such that a single writer sees data for multiple buckets so it must "group" data
   * by bucket ID (and then sort within each bucket as required) which is achieved via sorting
   * by {@link RecordIdentifier} which includes the {@link RecordIdentifier#getBucketProperty()}
   * which has the actual bucket ID in the high order bits.  This scheme also ensures that 
   * {@link org.apache.hadoop.hive.ql.exec.FileSinkOperator#process(Object, int)} works in case
   * there numBuckets > numReducers.  (The later could be fixed by changing how writers are
   * initialized in "if (fpaths.acidLastBucket != bucketNum) {")
   */
  V1(1) {
    @Override
    public int decodeWriterId(int bucketProperty) {
      return (bucketProperty & 0b0000_1111_1111_1111_0000_0000_0000_0000) >>> 16;
    }
    @Override
    public int decodeStatementId(int bucketProperty) {
      return (bucketProperty & 0b0000_0000_0000_0000_0000_1111_1111_1111);
    }
    @Override
    public int encode(AcidOutputFormat.Options options) {
      int statementId = options.getStatementId() >= 0 ? options.getStatementId() : 0;

      assert this.version >=0 && this.version <= MAX_VERSION
        : "Version out of range: " + version;
      if(!(options.getBucketId() >= 0 && options.getBucketId() <= MAX_BUCKET_ID)) {
        throw new IllegalArgumentException("bucketId out of range: " + options.getBucketId());
      }
      if(!(statementId >= 0 && statementId <= MAX_STATEMENT_ID)) {
        throw new IllegalArgumentException("statementId out of range: " + statementId);
      }
      return this.version << (1 + NUM_BUCKET_ID_BITS + 4 + NUM_STATEMENT_ID_BITS) |
        options.getBucketId() << (4 + NUM_STATEMENT_ID_BITS) | statementId;
    }
  };
  private static final int TOP3BITS_MASK = 0b1110_0000_0000_0000_0000_0000_0000_0000;
  private static final int NUM_VERSION_BITS = 3;
  private static final int NUM_BUCKET_ID_BITS = 12;
  private static final int NUM_STATEMENT_ID_BITS = 12;
  private static final int MAX_VERSION = (1 << NUM_VERSION_BITS) - 1;
  private static final int MAX_BUCKET_ID = (1 << NUM_BUCKET_ID_BITS) - 1;
  private static final int MAX_STATEMENT_ID = (1 << NUM_STATEMENT_ID_BITS) - 1;

  public static BucketCodec determineVersion(int bucket) {
    assert 7 << 29 == BucketCodec.TOP3BITS_MASK;
    //look at top 3 bits and return appropriate enum
    try {
      return getCodec((BucketCodec.TOP3BITS_MASK & bucket) >>> 29);
    }
    catch(IllegalArgumentException ex) {
      throw new IllegalArgumentException(ex.getMessage() + " Cannot decode version from " + bucket);
    }
  }
  public static BucketCodec getCodec(int version) {
    switch (version) {
      case 0:
        return BucketCodec.V0;
      case 1:
        return BucketCodec.V1;
      default:
        throw new IllegalArgumentException("Illegal 'bucket' format. Version=" + version);
    }
  }
  final int version;
  BucketCodec(int version) {
    this.version = version;
  }

  /**
   * For bucketed tables this the bucketId, otherwise writerId
   */
  public abstract int decodeWriterId(int bucketProperty);
  public abstract int decodeStatementId(int bucketProperty);
  public abstract int encode(AcidOutputFormat.Options options);
  public int getVersion() {
    return version;
  }
}
