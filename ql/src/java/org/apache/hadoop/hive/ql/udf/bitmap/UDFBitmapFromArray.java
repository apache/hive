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
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDFArrayBase;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils;
import org.apache.hadoop.hive.ql.util.bitmap.RoaringBitmapSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.LoggerFactory;

import org.slf4j.Logger;

import java.util.List;

import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.writableIntObjectInspector;

@Description(name = "bitmap_from_array", value = "_FUNC_(x) - Convert a int array to a BITMAP",
extended = "Example:\n > select _FUNC_(array(1,2,3));\n"
            + "binary")
public class UDFBitmapFromArray extends AbstractGenericUDFArrayBase implements Bitmap {
    public final static String FUNC_NAME = "bitmap_from_array";
    private static final Logger LOG = LoggerFactory.getLogger(UDFBitmapFromArray.class.getName());
    private transient ListObjectInspector arrayOI;
    private transient ObjectInspector arrayElementOI;

    protected UDFBitmapFromArray() {
        super(FUNC_NAME, 1, 1, null);
    }
    //  private transient Converter[] converters = new Converter[1];
    //  private transient PrimitiveCategory[] inputTypes = new PrimitiveCategory[1];

    @Override public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        //    obta(arguments, 0, inputTypes, converters);
        ObjectInspector initialize = super.initialize(arguments);
        arrayOI = (ListObjectInspector) arguments[0];
        arrayElementOI = arrayOI.getListElementObjectInspector();
        if (!ObjectInspectorUtils.compareTypes(arrayElementOI, writableIntObjectInspector)) {
            throw new UDFArgumentException("The first argument of function " + FUNC_NAME + " should be " +
                    "array<int> type, but " + arrayElementOI.getTypeName() + " was found");
        }
//        checkArgIntPrimitiveCategory((PrimitiveObjectInspector) arguments[ARRAY_IDX], FUNC_NAME, ARRAY_IDX);

        GenericUDFUtils.ReturnObjectInspectorResolver returnOIResolver =
                new GenericUDFUtils.ReturnObjectInspectorResolver(true);

        ObjectInspector elementObjectInspector =
                ((ListObjectInspector) (arguments[0])).getListElementObjectInspector();

        ObjectInspector returnOI = returnOIResolver.get(writableIntObjectInspector);
        converter = ObjectInspectorConverters.getConverter(elementObjectInspector, returnOI);

        return ObjectInspectorFactory.getStandardListObjectInspector(writableIntObjectInspector);
    }

    @Override public Object evaluate(DeferredObject[] arguments) throws HiveException {
        Object array = arguments[0].get();
        RoaringBitmap result = new RoaringBitmap();
        List<?> retArray = ((ListObjectInspector) argumentOIs[ARRAY_IDX]).getList(array);
        for (Object e : retArray) {
            try {
                result.add(checkBitIndex(((IntWritable) converter.convert(e)).get()));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
        return new BytesWritable(RoaringBitmapSerDe.serialize(result).array());
    }
    @Override public String getFuncName() {
        return FUNC_NAME;
    }
}