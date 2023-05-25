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
 * distributed under the License is distributed on an "import org.apache.hadoop.hive.ql.exec.FunctionInfo;
AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.udf.bitmap;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.ISupportStreamingModeForWindowing;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.apache.hadoop.hive.ql.udaf.TestStreamingSum.wdwFrame;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
import static org.junit.Assert.assertEquals;

public class TestUADFBitmap extends TestBitmapBase {
    public static <T, TW> void _agg(GenericUDAFResolver fnR, TypeInfo[] inputTypes, Iterator<T> inVals, ObjectInspector[] inputOIs, int inSz, int numPreceding, int numFollowing, Iterator<TW> outVals) throws HiveException {
        GenericUDAFEvaluator fn = fnR.getEvaluator(inputTypes);
        fn.init(GenericUDAFEvaluator.Mode.COMPLETE, inputOIs);
        fn = fn.getWindowingEvaluator(wdwFrame(numPreceding, numFollowing));
        GenericUDAFEvaluator.AggregationBuffer agg = fn.getNewAggregationBuffer();
        ISupportStreamingModeForWindowing oS = (ISupportStreamingModeForWindowing) fn;
        int outSz = 0;
        while (inVals.hasNext()) {
            fn.aggregate(agg, new Object[]{inVals.next()});
            Object out = oS.getNextResult(agg);
            if (out != null) {
                if (out == ISupportStreamingModeForWindowing.NULL_RESULT) {
                    out = null;
                }
                Assert.assertEquals(out, outVals.next());
                outSz++;
            }
        }
        fn.terminate(agg);
        while (outSz < inSz) {
            Object out = oS.getNextResult(agg);
            if (out == ISupportStreamingModeForWindowing.NULL_RESULT) {
                out = null;
            }
            Assert.assertEquals(out, outVals.next());
            outSz++;
        }
    }

    public void testUDAFToBitmapWindow(Iterator<IntWritable> inVals, int inSz, int numPreceding, int numFollowing, Iterator<BytesWritable> outVals) throws HiveException {
        UDAFToBitmap fnR = new UDAFToBitmap();
        TypeInfo[] inputTypes = {TypeInfoFactory.intTypeInfo};
        ObjectInspector[] inputOIs = {PrimitiveObjectInspectorFactory.writableIntObjectInspector};
        _agg(fnR, inputTypes, inVals, inputOIs, inSz, numPreceding, numFollowing, outVals);
    }

    @Test public void testToBitmap_3_4() throws HiveException {
        List<IntWritable> inVals = Arrays.asList(new IntWritable(1), new IntWritable(2), new IntWritable(3), new IntWritable(4), new IntWritable(5), new IntWritable(6), new IntWritable(7), new IntWritable(8), new IntWritable(9), new IntWritable(10));
        List<BytesWritable> outVals = Arrays.asList((BytesWritable) getBitmapFromInts(1, 2, 3, 4, 5), (BytesWritable) getBitmapFromInts(1, 2, 3, 4, 5, 6), (BytesWritable) getBitmapFromInts(1, 2, 3, 4, 5, 6, 7), (BytesWritable) getBitmapFromInts(1, 2, 3, 4, 5, 6, 7, 8), (BytesWritable) getBitmapFromInts(2, 3, 4, 5, 6, 7, 8, 9), (BytesWritable) getBitmapFromInts(3, 4, 5, 6, 7, 8, 9, 10), (BytesWritable) getBitmapFromInts(4, 5, 6, 7, 8, 9, 10), (BytesWritable) getBitmapFromInts(5, 6, 7, 8, 9, 10), (BytesWritable) getBitmapFromInts(6, 7, 8, 9, 10), (BytesWritable) getBitmapFromInts(7, 8, 9, 10));
        testUDAFToBitmapWindow(inVals.iterator(), 10, 3, 4, outVals.iterator());
    }

    @Test public void testUDAFToBitMap() throws HiveException {
        List<List<Object>> invals = Arrays.asList(Arrays.asList(1, 2, 3), Arrays.asList(1, 2, 3));
        Object o = testUDAFBitMap(new UDAFToBitmap(), new TypeInfo[]{TypeInfoFactory.intTypeInfo}, new ObjectInspector[]{PrimitiveObjectInspectorFactory.javaIntObjectInspector}, new ObjectInspector[]{PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector}, invals);
        assertEquals(o, getBitmapFromInts(1, 2, 3));
    }

    @Test public void testUDAFBitmapUnion() throws HiveException {
        List<List<Object>> invals = Arrays.asList(Arrays.asList(getBitmapFromInts(1, 2)), Arrays.asList(getBitmapFromInts(2, 3)));
        Object o = testUDAFBitMap(new UDAFBitmapUnion(), new TypeInfo[]{writableBinaryObjectInspector.getTypeInfo()}, new ObjectInspector[]{PrimitiveObjectInspectorFactory.javaIntObjectInspector}, new ObjectInspector[]{PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector}, invals);
        assertEquals(o, getBitmapFromInts(1, 2, 3));
    }

    @Test public void testUDAFBitmapintersect() throws HiveException {
        List<List<Object>> invals = Arrays.asList(Arrays.asList(getBitmapFromInts(1, 2)), Arrays.asList(getBitmapFromInts(2, 3)));
        Object o = testUDAFBitMap(new UDAFBitmapIntersect(), new TypeInfo[]{writableBinaryObjectInspector.getTypeInfo()}, new ObjectInspector[]{PrimitiveObjectInspectorFactory.javaIntObjectInspector}, new ObjectInspector[]{PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector}, invals);
        assertEquals(o, getBitmapFromInts(2));
    }

    public Object testUDAFBitMap(AbstractGenericUDAFResolver resolver, TypeInfo[] infos, ObjectInspector[] inputIO, ObjectInspector[] outputIO, List<List<Object>> inVals) throws HiveException {
        ArrayList<GenericUDAFEvaluator> evals = new ArrayList<>(inVals.size());
        ArrayList<ObjectInspector> ois = new ArrayList<>(inVals.size());
        ArrayList<Object> partialResult = new ArrayList<>(inVals.size());
        ArrayList<GenericUDAFEvaluator.AggregationBuffer> buffers = new ArrayList<>(inVals.size());
        for (List<Object> inVal : inVals) {
            GenericUDAFEvaluator evaluator = resolver.getEvaluator(infos);
            ObjectInspector oi = evaluator.init(GenericUDAFEvaluator.Mode.PARTIAL1, inputIO);
            GenericUDAFEvaluator.AggregationBuffer buffer = evaluator.getNewAggregationBuffer();
            for (Object o : inVal) {
                evaluator.iterate(buffer, new Object[]{o});
            }
            Object object = evaluator.terminatePartial(buffer);
            buffers.add(buffer);
            partialResult.add(object);
            evals.add(evaluator);
            ois.add(oi);
        }
        GenericUDAFEvaluator genericUDAFEvaluator = evals.get(0);
        ObjectInspector oiP = genericUDAFEvaluator.init(GenericUDAFEvaluator.Mode.FINAL, outputIO);
        GenericUDAFEvaluator.AggregationBuffer newAggregationBuffer = buffers.get(0);
        for (Object o : partialResult) {
            genericUDAFEvaluator.merge(newAggregationBuffer, o);
        }
        Object result = genericUDAFEvaluator.terminate(newAggregationBuffer);
        return result;
    }
}