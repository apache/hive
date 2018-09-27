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
  public enum Field {
    //note the enum names match field names in the struct
    writeId(TypeInfoFactory.longTypeInfo,
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
      struct[Field.writeId.ordinal()] = ri.getWriteId();
      struct[Field.bucketId.ordinal()] = ri.getBucketProperty();
      struct[Field.rowId.ordinal()] = ri.getRowId();
    }
  }
  
  private long writeId;
  private int bucketId;
  private long rowId;

  public RecordIdentifier() {
  }

  public RecordIdentifier(long writeId, int bucket, long rowId) {
    this.writeId = writeId;
    this.bucketId = bucket;
    this.rowId = rowId;
  }

  /**
   * Set the identifier.
   * @param writeId the write id
   * @param bucketId the bucket id
   * @param rowId the row id
   */
  public void setValues(long writeId, int bucketId, long rowId) {
    this.writeId = writeId;
    this.bucketId = bucketId;
    this.rowId = rowId;
  }

  /**
   * Set this object to match the given object.
   * @param other the object to copy from
   */
  public void set(RecordIdentifier other) {
    this.writeId = other.writeId;
    this.bucketId = other.bucketId;
    this.rowId = other.rowId;
  }

  public void setRowId(long rowId) {
    this.rowId = rowId;
  }

  /**
   * What was the original write id for the last row?
   * @return the write id
   */
  public long getWriteId() {
    return writeId;
  }

  /**
   * See {@link BucketCodec} for details
   * @return the bucket value;
   */
  public int getBucketProperty() {
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
    if (writeId != other.writeId) {
      return writeId < other.writeId ? -1 : 1;
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
      //WTF?  assumes that other instanceof OrcRawRecordMerger.ReaderKey???
      return -other.compareTo(this);
    }
    return compareToInternal(other);
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeLong(writeId);
    dataOutput.writeInt(bucketId);
    dataOutput.writeLong(rowId);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    writeId = dataInput.readLong();
    bucketId = dataInput.readInt();
    rowId = dataInput.readLong();
  }

  @Override
  public boolean equals(Object other) {
    if(other == this) {
      return true;
    }
    if (other == null || other.getClass() != getClass()) {
      return false;
    }
    RecordIdentifier oth = (RecordIdentifier) other;
    return oth.writeId == writeId &&
        oth.bucketId == bucketId &&
        oth.rowId == rowId;
  }
  @Override
  public int hashCode() {
    int result = 17;
    result = 31 * result + (int)(writeId ^ (writeId >>> 32));
    result = 31 * result + bucketId;
    result = 31 * result + (int)(rowId ^ (rowId >>> 32));
    return result;
  }

  @Override
  public String toString() {
    return "RecordIdentifier(" + writeId + ", " + bucketToString(bucketId) + ","
        + getRowId() +")";
  }
  public static String bucketToString(int bucketId) {
    if (bucketId == -1) return "" + bucketId;
    BucketCodec codec =
      BucketCodec.determineVersion(bucketId);
    return  bucketId + "(" + codec.getVersion() + "." +
        codec.decodeWriterId(bucketId) + "." +
        codec.decodeStatementId(bucketId) + ")";
  }
}
