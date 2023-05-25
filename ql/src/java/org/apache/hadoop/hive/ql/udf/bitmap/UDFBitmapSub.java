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

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFParamUtils;
import org.apache.hadoop.hive.ql.util.bitmap.RoaringBitmapSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.BitSet;

import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.BINARY_GROUP;

@Description(name = "bitmap_sub", value = "_FUNC_(start[,end]) - Return subset in specified range (not include the range_end).",
extended = "Example:\n > select bitmap_to_string(_FUNC_(bitmap(0,1,3,4,5,6,7,8,9,10),0,5)) from src limit 1;\n '0,1,3,4'"
)
public class UDFBitmapSub extends GenericUDF implements Bitmap{
    public final static String FUNC_NAME = "bitmap_sub";
    private static final Logger LOG = LoggerFactory.getLogger(UDFBitmapSub.class.getName());
    private transient ObjectInspectorConverters.Converter[] converters = null;
    private transient PrimitiveObjectInspector.PrimitiveCategory[] inputTypes = null;
    private transient int length= 0;
    @Override public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        checkArgsSize(arguments, 2, 3);
        length = arguments.length;
        converters = new ObjectInspectorConverters.Converter[length];
        inputTypes = new PrimitiveObjectInspector.PrimitiveCategory[length];
        checkArgGroups(arguments, 0, inputTypes, BINARY_GROUP);
        GenericUDFParamUtils.obtainBinaryConverter(arguments, 0, inputTypes, converters);
        for (int i = 1; i < length; i++) {
            obtainLongConverter(arguments, i, inputTypes, converters);
        }
        return writableBinaryObjectInspector;
    }

    @Override public Object evaluate(DeferredObject[] arguments) throws HiveException {
        try {
            RoaringBitmap result = RoaringBitmapSerDe.deserialize(GenericUDFParamUtils.getBinaryValue(arguments, 0, converters).getBytes());
            Long start = getLongValue(arguments, 1, converters);
            Long end = length==2?result.getCardinality():getLongValue(arguments, 2, converters);
            checkBitRange(start, end);
            RoaringBitmap and = new RoaringBitmap();
            and.add(start, end);
            result.and(and);
            return new BytesWritable(RoaringBitmapSerDe.serialize(result).array());
        } catch (IOException e) {
            throw new HiveException(e);
        }
    }

    @Override public String getDisplayString(String[] children) {
        return getStandardDisplayString(FUNC_NAME, children, ",");
    }
    @Override public String getFuncName() {
        return FUNC_NAME;
    }
}