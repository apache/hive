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
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Gives the Record identifier information for the current record.
 */
public class RecordIdentifier implements WritableComparable<RecordIdentifier> {
  /**
   * This is in support of {@link org.apache.hadoop.hive.ql.metadata.VirtualColumn#ROWID}
   * Contains metadata about each field in RecordIdentifier that needs to be part of ROWID
   * which is represented as a struct {@link org.apache.hadoop.hive.ql.io.RecordIdentifier.StructInfo}.
   * Each field of RecordIdentifier which should be part of ROWID should be in this enum... which 
   * really means that it should be part of VirtualColumn (so make a subclass for rowid).
   */
  public static enum Field {
    //note the enum names match field names in the struct
    transactionId(TypeInfoFactory.longTypeInfo,
      PrimitiveObjectInspectorFactory.javaLongObjectInspector),
    bucketId(TypeInfoFactory.intTypeInfo, PrimitiveObjectInspectorFactory.javaIntObjectInspector),
    rowId(TypeInfoFactory.longTypeInfo, PrimitiveObjectInspectorFactory.javaLongObjectInspector);
    public final TypeInfo fieldType;
    public final ObjectInspector fieldOI;
    Field(TypeInfo fieldType, ObjectInspector fieldOI) {
      this.fieldType = fieldType;
      this.fieldOI = fieldOI;
    }
  }
  /**
   * RecordIdentifier is passed along the operator tree as a struct.  This class contains a few
   * utilities for that.
   */
  public static final class StructInfo {
    private static final List<String> fieldNames = new ArrayList<String>(Field.values().length);
    private static final List<TypeInfo> fieldTypes = new ArrayList<TypeInfo>(fieldNames.size());
    private static final List<ObjectInspector> fieldOis = 
      new ArrayList<ObjectInspector>(fieldNames.size());
    static {
      for(Field f : Field.values()) {
        fieldNames.add(f.name());
        fieldTypes.add(f.fieldType);
        fieldOis.add(f.fieldOI);
      }
    }
    public static final TypeInfo typeInfo = 
      TypeInfoFactory.getStructTypeInfo(fieldNames, fieldTypes);
    public static final ObjectInspector oi = 
      ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOis);

    /**
     * Copies relevant fields from {@code ri} to {@code struct}
     * @param ri
     * @param struct must be of size Field.values().size()
     */
    public static void toArray(RecordIdentifier ri, Object[] struct) {
      assert struct != null && struct.length == Field.values().length;
      if(ri == null) {
        Arrays.fill(struct, null);
        return;
      }
      struct[Field.transactionId.ordinal()] = ri.getTransactionId();
      struct[Field.bucketId.ordinal()] = ri.getBucketId();
      struct[Field.rowId.ordinal()] = ri.getRowId();
    }
  }
  
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
