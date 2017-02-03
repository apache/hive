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

package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedExpressions;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorInBloomFilterColDynamicValue;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hive.common.util.BloomFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.sql.Timestamp;

/**
 * GenericUDF to lookup a value in BloomFilter
 */
@VectorizedExpressions({VectorInBloomFilterColDynamicValue.class})
public class GenericUDFInBloomFilter extends GenericUDF {
  private static final Logger LOG = LoggerFactory.getLogger(GenericUDFInBloomFilter.class);

  private transient ObjectInspector valObjectInspector;
  private transient ObjectInspector bloomFilterObjectInspector;
  private transient BloomFilter bloomFilter;
  private transient boolean initializedBloomFilter;
  private transient byte[] scratchBuffer = new byte[HiveDecimal.SCRATCH_BUFFER_LEN_TO_BYTES];

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 2) {
      throw new UDFArgumentLengthException(
              "InBloomFilter requires exactly 2 arguments but got " + arguments.length);
    }

    // Verify individual arguments
    if (arguments[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentTypeException(0, "The 1st argument must be a primitive type but "
      + arguments[0].getTypeName() + " was passed");
    }

    if (((PrimitiveObjectInspector) arguments[1]).getPrimitiveCategory() !=
            PrimitiveObjectInspector.PrimitiveCategory.BINARY) {
      throw new UDFArgumentTypeException(1, "The 2nd argument must be a binary type but " +
      arguments[1].getTypeName() + " was passed");
    }

    valObjectInspector = arguments[0];
    bloomFilterObjectInspector = arguments[1];
    assert bloomFilterObjectInspector instanceof WritableBinaryObjectInspector;

    initializedBloomFilter = false;
    return PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
  }

  @Override
  public String getDisplayString(String[] children) {
    return getStandardDisplayString("in_bloom_filter", children);
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    // Return if either of the arguments is null
    if (arguments[0].get() == null || arguments[1].get() == null) {
      return null;
    }

    if (!initializedBloomFilter) {
      // Setup the bloom filter once
      try {
        BytesWritable bw = (BytesWritable) arguments[1].get();
        byte[] bytes = new byte[bw.getLength()];
        System.arraycopy(bw.getBytes(), 0, bytes, 0, bw.getLength());
        bloomFilter = BloomFilter.deserialize(new ByteArrayInputStream(bytes));
      } catch ( IOException e) {
        throw new HiveException(e);
      }
      initializedBloomFilter = true;
    }

    // Check if the value is in bloom filter
    switch (((PrimitiveObjectInspector)valObjectInspector).
            getTypeInfo().getPrimitiveCategory()) {
      case BOOLEAN:
        boolean vBoolean = ((BooleanObjectInspector)valObjectInspector).
                get(arguments[0].get());
        return bloomFilter.testLong(vBoolean ? 1 : 0);
      case BYTE:
        byte vByte = ((ByteObjectInspector) valObjectInspector).
                get(arguments[0].get());
        return bloomFilter.testLong(vByte);
      case SHORT:
        short vShort = ((ShortObjectInspector) valObjectInspector).
                get(arguments[0].get());
        return bloomFilter.testLong(vShort);
      case INT:
        int vInt = ((IntObjectInspector) valObjectInspector).
                get(arguments[0].get());
        return bloomFilter.testLong(vInt);
      case LONG:
        long vLong = ((LongObjectInspector) valObjectInspector).
                get(arguments[0].get());
        return bloomFilter.testLong(vLong);
      case FLOAT:
        float vFloat = ((FloatObjectInspector) valObjectInspector).
                get(arguments[0].get());
        return  bloomFilter.testDouble(vFloat);
      case DOUBLE:
        double vDouble = ((DoubleObjectInspector) valObjectInspector).
                get(arguments[0].get());
        return bloomFilter.testDouble(vDouble);
      case DECIMAL:
        HiveDecimalWritable vDecimal = ((HiveDecimalObjectInspector) valObjectInspector).
            getPrimitiveWritableObject(arguments[0].get());
        int startIdx = vDecimal.toBytes(scratchBuffer);
        return bloomFilter.testBytes(scratchBuffer, startIdx, scratchBuffer.length - startIdx);
      case DATE:
        DateWritable vDate = ((DateObjectInspector) valObjectInspector).
                getPrimitiveWritableObject(arguments[0].get());
        return bloomFilter.testLong(vDate.getDays());
      case TIMESTAMP:
        Timestamp vTimeStamp = ((TimestampObjectInspector) valObjectInspector).
                getPrimitiveJavaObject(arguments[0].get());
        return bloomFilter.testLong(vTimeStamp.getTime());
      case CHAR:
        Text vChar = ((HiveCharObjectInspector) valObjectInspector).
                getPrimitiveWritableObject(arguments[0].get()).getStrippedValue();
        return bloomFilter.testBytes(vChar.getBytes(), 0, vChar.getLength());
      case VARCHAR:
        Text vVarchar = ((HiveVarcharObjectInspector) valObjectInspector).
                getPrimitiveWritableObject(arguments[0].get()).getTextValue();
        return bloomFilter.testBytes(vVarchar.getBytes(), 0, vVarchar.getLength());
      case STRING:
        Text vString = ((StringObjectInspector) valObjectInspector).
                getPrimitiveWritableObject(arguments[0].get());
        return bloomFilter.testBytes(vString.getBytes(), 0, vString.getLength());
      case BINARY:
        BytesWritable vBytes = ((BinaryObjectInspector) valObjectInspector).
                getPrimitiveWritableObject(arguments[0].get());
        return bloomFilter.testBytes(vBytes.getBytes(), 0, vBytes.getLength());
      default:
        throw new UDFArgumentTypeException(0, "Bad primitive category " +
                ((PrimitiveTypeInfo) valObjectInspector).getPrimitiveCategory());
    }
  }
}
