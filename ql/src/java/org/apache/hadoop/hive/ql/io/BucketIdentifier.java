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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stores bucket and writeId of the bucket files.
 */
public class BucketIdentifier {

  private static final Logger LOG = LoggerFactory.getLogger(BucketIdentifier.class);

  public static BucketIdentifier from(Configuration conf, Path path) {
    if (!AcidUtils.isInsertOnlyFetchBucketId(conf)) {
      return null;
    }

    return BucketIdentifier.parsePath(path);
  }

  public static BucketIdentifier parsePath(Path path) {
    try {
      Path parent = path.getParent();
      if (parent == null) {
        return null;
      }
      boolean isBase = parent.getName().startsWith(AcidUtils.BASE_PREFIX);
      boolean isDelta = parent.getName().startsWith(AcidUtils.DELTA_PREFIX)
          || parent.getName().startsWith(AcidUtils.DELETE_DELTA_PREFIX);
      if (isBase || isDelta) {
        if (isBase) {
          AcidUtils.ParsedBaseLight parsedBaseLight = AcidUtils.ParsedBaseLight.parseBase(parent);
          if (parsedBaseLight.getVisibilityTxnId() > 0) {
            // This file is a result of compaction. It may contain records from more than one write.
            return null;
          }
          return new BucketIdentifier(
              parsedBaseLight.getWriteId(),
              AcidUtils.parseBucketId(path));
        } else {
          AcidUtils.ParsedDeltaLight pd = AcidUtils.ParsedDeltaLight.parse(parent);
          if (pd.getMinWriteId() != pd.getMaxWriteId()) {
            // This file is a result of compaction. It contains records from more than one write.
            return null;
          }
          return new BucketIdentifier(
              pd.getMinWriteId(),
              AcidUtils.parseBucketId(path));
        }
      }
    } catch (NumberFormatException ex) {
      LOG.warn("Error while parsing path " + path, ex);
    }

    return null;
  }

  private final long writeId;
  private final int bucketId;

  public BucketIdentifier(long writeId, int bucket) {
    this.writeId = writeId;
    this.bucketId = bucket;
  }

  public long getWriteId() {
    return writeId;
  }

  public int getBucketProperty() {
    return bucketId;
  }

  @Override
  public String toString() {
    return "BucketIdentifier{" +
        "writeId=" + writeId +
        ", bucketId=" + bucketId +
        '}';
  }
}
