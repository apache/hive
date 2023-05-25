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
package org.apache.hadoop.hive.ql.udf.bitmap;

import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.roaringbitmap.RoaringBitmap;

import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.BINARY_GROUP;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.NUMERIC_GROUP;

public interface Bitmap {
    default Integer checkBitIndex(Integer bitIndex) {
        if (bitIndex < 0)
            throw new IndexOutOfBoundsException("bitIndex < 0: " + bitIndex);
        return bitIndex;
    }

    default Integer checkBitIndex(String bitIndex) {
        Integer integer = Integer.valueOf(bitIndex);
        if (integer < 0)
            throw new IndexOutOfBoundsException("bitIndex < 0: " + bitIndex);
        return integer;
    }

    default void checkBitRange(Long start, Long end) {
        if (start < 0 || end > Integer.MAX_VALUE + 1l) {
            throw new IndexOutOfBoundsException("range Index < 0 or >2147483647, start : " + start + ", end : " + end);
        }
    }

    default void checkBinaryParameters(TypeInfo[] parameters, String funcName) throws UDFArgumentLengthException, UDFArgumentTypeException {
        if (parameters.length != 1) {
            throw new UDFArgumentLengthException("The function " + funcName + " accepts 1 arguments.");
        }
        if (parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE || PrimitiveObjectInspectorUtils.getPrimitiveGrouping(((PrimitiveTypeInfo) parameters[0]).getPrimitiveCategory()) != BINARY_GROUP) {
            throw new UDFArgumentTypeException(0, "The function " + funcName + "'s 1st param must be binary, but input is " + parameters[0].getTypeName());
        }
    }

    default void checkNumberParameters(TypeInfo[] parameters, String funcName) throws UDFArgumentLengthException, UDFArgumentTypeException {
        if (parameters.length != 1) {
            throw new UDFArgumentLengthException("The function " + funcName + " accepts 1 arguments.");
        }
        if (parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE || PrimitiveObjectInspectorUtils.getPrimitiveGrouping(((PrimitiveTypeInfo) parameters[0]).getPrimitiveCategory()) != NUMERIC_GROUP) {
            throw new UDFArgumentTypeException(0, "The function " + funcName + "'s 1st param must be binary, but input is " + parameters[0].getTypeName());
        }
    }

    public static class BitmapDataAggregationBuffer extends GenericUDAFEvaluator.AbstractAggregationBuffer {
        private boolean isFirst = true;
        RoaringBitmap bitmap = new RoaringBitmap();

        @Override public int estimate() {
            return bitmap.getSizeInBytes();
        }

        public boolean isFirstAccess() {
            try {
                return isFirst;
            } finally {
                isFirst = false;
            }
        }
    }
}