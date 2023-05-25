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
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFParamUtils;
import org.apache.hadoop.hive.ql.util.bitmap.RoaringBitmapSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableBinaryObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.BytesWritable;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;

@Description(name = "bitmap_union"
        , value = "_FUNC_(x) - Aggregate function, used to calculate the grouped bitmap union.",
extended = "Example:\n >  select  city,_FUNC_(user_id_bitmap) from table group by city\n"
        + "binary"
)
@SuppressWarnings("deprecation")
public class UDAFBitmapUnion extends AbstractGenericUDAFResolver implements Bitmap{

    private static final Logger LOG = LoggerFactory.getLogger(UDAFBitmapUnion.class.getName());
    public final static String FUNC_NAME = "bitmap_union";

    @Override public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
        checkBinaryParameters(parameters,FUNC_NAME);
        return new RoaringBitmapEvaluator();
    }

    public static class RoaringBitmapEvaluator extends GenericUDAFEvaluator {

        private Mode mode;

        private WritableBinaryObjectInspector midOI = writableBinaryObjectInspector;
        private transient ObjectInspectorConverters.Converter[] converters = new ObjectInspectorConverters.Converter[1];
        private transient PrimitiveObjectInspector.PrimitiveCategory[] inputTypes = new PrimitiveObjectInspector.PrimitiveCategory[1];

        @Override public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            assert (parameters.length == 1);
            super.init(m, parameters);
            mode = m;
            GenericUDFParamUtils.obtainBinaryConverter(parameters, 0, inputTypes, converters);
            return writableBinaryObjectInspector;
        }

        @Override public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            return new BitmapDataAggregationBuffer();
        }

        @Override public void reset(AggregationBuffer agg) throws HiveException {
            ((BitmapDataAggregationBuffer) agg).bitmap.clear();
        }

        @Override public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            try {
                ((BitmapDataAggregationBuffer) agg).bitmap.or(RoaringBitmapSerDe.deserialize(midOI.getPrimitiveJavaObject(parameters[0])));
            } catch (IOException e) {
                throw new HiveException(e);
            }
        }

        @Override public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            return new BytesWritable(RoaringBitmapSerDe.serialize(((BitmapDataAggregationBuffer) agg).bitmap).array());
        }

        @Override public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            try {
                ((BitmapDataAggregationBuffer) agg).bitmap.or(RoaringBitmapSerDe.deserialize(midOI.getPrimitiveJavaObject(partial)));
            } catch (IOException e) {
                throw new HiveException(e);
            }
        }

        @Override public Object terminate(AggregationBuffer agg) throws HiveException {
            return new BytesWritable(RoaringBitmapSerDe.serialize(((BitmapDataAggregationBuffer) agg).bitmap).array());
        }
    }
}