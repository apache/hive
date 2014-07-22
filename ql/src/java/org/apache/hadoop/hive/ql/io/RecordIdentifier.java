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

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Gives the Record identifer information for the current record.
 */
public class RecordIdentifier implements WritableComparable<RecordIdentifier> {
  private long transactionId;
  private int bucketId;
  private long rowId;

  public RecordIdentifier() {
  }

  public RecordIdentifier(long transactionId, int bucket, long rowId) {
    this.transactionId = transactionId;
    this.bucketId = bucket;
    this.rowId = rowId;
  }

  /**
   * Set the identifier.
   * @param transactionId the transaction id
   * @param bucketId the bucket id
   * @param rowId the row id
   */
  public void setValues(long transactionId, int bucketId, long rowId) {
    this.transactionId = transactionId;
    this.bucketId = bucketId;
    this.rowId = rowId;
  }

  /**
   * Set this object to match the given object.
   * @param other the object to copy from
   */
  public void set(RecordIdentifier other) {
    this.transactionId = other.transactionId;
    this.bucketId = other.bucketId;
    this.rowId = other.rowId;
  }

  public void setRowId(long rowId) {
    this.rowId = rowId;
  }

  /**
   * What was the original transaction id for the last row?
   * @return the transaction id
   */
  public long getTransactionId() {
    return transactionId;
  }

  /**
   * What was the original bucket id for the last row?
   * @return the bucket id
   */
  public int getBucketId() {
    return bucketId;
  }

  /**
   * What was the original row id for the last row?
   * @return the row id
   */
  public long getRowId() {
    return rowId;
  }

  protected int compareToInternal(RecordIdentifier other) {
    if (other == null) {
      return -1;
    }
    if (transactionId != other.transactionId) {
      return transactionId < other.transactionId ? -1 : 1;
    }
    if (bucketId != other.bucketId) {
      return bucketId < other.bucketId ? - 1 : 1;
    }
    if (rowId != other.rowId) {
      return rowId < other.rowId ? -1 : 1;
    }
    return 0;
  }

  @Override
  public int compareTo(RecordIdentifier other) {
    if (other.getClass() != RecordIdentifier.class) {
      return -other.compareTo(this);
    }
    return compareToInternal(other);
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeLong(transactionId);
    dataOutput.writeInt(bucketId);
    dataOutput.writeLong(rowId);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    transactionId = dataInput.readLong();
    bucketId = dataInput.readInt();
    rowId = dataInput.readLong();
  }

  @Override
  public boolean equals(Object other) {
    if (other == null || other.getClass() != getClass()) {
      return false;
    }
    RecordIdentifier oth = (RecordIdentifier) other;
    return oth.transactionId == transactionId &&
        oth.bucketId == bucketId &&
        oth.rowId == rowId;
  }

  @Override
  public String toString() {
    return "{originalTxn: " + transactionId + ", bucket: " +
        bucketId + ", row: " + getRowId() + "}";
  }
}
