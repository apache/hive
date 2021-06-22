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
package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.io.BucketCodec;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.LongWritable;

/**
 * GenericUDFValidateAcidSortOrder.
 */
@UDFType(stateful=true)
@Description(name = "validate_acid_sort_order(ROW__ID.writeId, ROW__ID.bucketId, ROW__ID.rowId)", 
  value = "_FUNC_(writeId, bucketId, rowId) - returns 0 if the current row is in the right acid sort order "
    + "compared to the previous row")
public class GenericUDFValidateAcidSortOrder extends GenericUDF {
  public static final String UDF_NAME = "validate_acid_sort_order";
  private transient PrimitiveCategory[] inputTypes = new PrimitiveCategory[3];
  private transient Converter[] converters = new Converter[3];
  private final LongWritable output = new LongWritable();
  // See {@link org.apache.hadoop.hive.ql.exec.tez.SplitGrouper#getCompactorGroups}
  // Each writer is handling only one logical bucket (i.e. all files with same bucket number end up in one writer)
  private int bucketNumForWriter = -1;
  private WriteIdRowId previousWriteIdRowId = null;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    checkArgsSize(arguments, 3, 3);
    checkArgPrimitive(arguments, 0);
    checkArgPrimitive(arguments, 1);
    checkArgPrimitive(arguments, 2);
    obtainLongConverter(arguments, 0, inputTypes, converters);
    obtainIntConverter(arguments, 1, inputTypes, converters);
    obtainLongConverter(arguments, 2, inputTypes, converters);
    ObjectInspector outputOI = PrimitiveObjectInspectorFactory.writableLongObjectInspector;
    return outputOI;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    long writeId = getLongValue(arguments, 0, converters);
    int bucketProperty = getIntValue(arguments, 1, converters);
    int bucketNum = BucketCodec.determineVersion(bucketProperty).decodeWriterId(bucketProperty);
    long rowId = getLongValue(arguments, 2, converters);
    if (bucketNumForWriter < 0) {
      bucketNumForWriter = bucketNum;
    } else {
      if (bucketNumForWriter != bucketNum) {
        throw new HiveException("One writer is supposed to handle only one bucket. We saw these 2 different buckets: "
            + bucketNumForWriter + " and " + bucketNum);
      }
    }
    WriteIdRowId current = new WriteIdRowId(bucketProperty, writeId, rowId);
    if (previousWriteIdRowId != null) {
      // Verify sort order for this new row
      if (current.compareTo(previousWriteIdRowId) <= 0) {
        throw new HiveException("Wrong sort order of Acid rows detected for the rows: "
            + previousWriteIdRowId.toString() + " and " + current.toString());
      }
    }
    previousWriteIdRowId = current;
    output.set(0);
    return output;
  }

  @Override
  public String getDisplayString(String[] children) {
    return getStandardDisplayString("validate_acid_sort_order", children);
  }

  final static class WriteIdRowId implements Comparable<WriteIdRowId> {
    final int bucketProperty;
    final long writeId;
    final long rowId;

    WriteIdRowId(int bucketProperty, long writeId, long rowId) {
      this.bucketProperty = bucketProperty;
      this.writeId = writeId;
      this.rowId = rowId;
    }

    @Override
    public int compareTo(WriteIdRowId other) {
      if (this.writeId != other.writeId) {
        return this.writeId < other.writeId ? -1 : 1;
      }
      if (this.bucketProperty != other.bucketProperty) {
        return this.bucketProperty < other.bucketProperty ? -1 : 1;
      }
      if (this.rowId != other.rowId) {
        return this.rowId < other.rowId ? -1 : 1;
      }
      return 0;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("[writeId=");
      sb.append(writeId);
      sb.append(", bucketProperty=");
      sb.append(bucketProperty);
      sb.append(", rowId=");
      sb.append(rowId);
      sb.append("]");
      return sb.toString();
    }
  }
}