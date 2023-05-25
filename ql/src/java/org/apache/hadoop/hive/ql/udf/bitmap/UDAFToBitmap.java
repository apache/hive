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
import org.apache.hadoop.hive.ql.plan.ptf.WindowFrameDef;
import org.apache.hadoop.hive.ql.udf.generic.*;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.hadoop.hive.ql.util.bitmap.RoaringBitmapSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaIntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableBinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableIntObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;

@Description(name = "to_bitmap", value = "_FUNC_(x) - Convert an int (ranging from 0 to Integer.MAX_VALUE) to a bitmap containing that value. Mainly be used to load integer value into bitmap column.", extended = " Example:\n > select city,_FUNC_(id) from src group by city;\n")
@SuppressWarnings("deprecation")
public class UDAFToBitmap extends AbstractGenericUDAFResolver  implements Bitmap{
    public final static String FUNC_NAME = "to_bitmap";
    private static final Logger LOG = LoggerFactory.getLogger(UDAFToBitmap.class.getName());

    @Override public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
        checkNumberParameters(parameters,FUNC_NAME);
        return new RoaringBitmapEvaluator();
    }

        public static class RoaringBitmapEvaluator extends GenericUDAFEvaluator implements Bitmap{

        private Mode mode;

        private PrimitiveObjectInspector inputOI;

        private WritableBinaryObjectInspector midOI = writableBinaryObjectInspector,
                outputOI = writableBinaryObjectInspector;

        @Override public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m, parameters);
            mode = m;
            if (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {
                inputOI = (PrimitiveObjectInspector) parameters[0];
                PrimitiveObjectInspector actualOI = (PrimitiveObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(inputOI, ObjectInspectorUtils.ObjectInspectorCopyOption.JAVA);
                if (!(actualOI instanceof JavaIntObjectInspector)) {
                    throw new HiveException(FUNC_NAME + " only accept int variable.");
                }
            }
            return writableBinaryObjectInspector;
        }

        @Override public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            return new BitmapDataAggregationBuffer();
        }

        @Override public void reset(AggregationBuffer agg) throws HiveException {
            ((BitmapDataAggregationBuffer) agg).bitmap.clear();
        }

        @Override public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            ((BitmapDataAggregationBuffer) agg).bitmap.add(checkBitIndex(((IntObjectInspector) inputOI).get(parameters[0])));
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
        @Override
        public GenericUDAFEvaluator getWindowingEvaluator(WindowFrameDef wFrmDef) {
            return new UDAFToBitmap.ToBitMapStreamingFixedWindow(this, wFrmDef);
        }



    }
    static class  ToBitMapStreamingFixedWindow extends GenericUDAFStreamingEvaluator<Object> {

        class State extends GenericUDAFStreamingEvaluator<Object>.StreamingState {
            private Deque<Object> chain;
            private Map<Object, Integer> bitMap;
            public State(AggregationBuffer buf) {
                super(buf);
                if (!wFrameDef.isStartUnbounded()){
                    chain = new ArrayDeque<Object>(wFrameDef.getWindowSize());
                    bitMap = new LinkedHashMap<>();
                }
            }

            @Override
            public int estimate() {
                if (!(wrappedBuf instanceof AbstractAggregationBuffer)) {
                    return -1;
                }
                int underlying = ((AbstractAggregationBuffer) wrappedBuf).estimate();
                if (underlying == -1) {
                    return -1;
                }
                if (wFrameDef.isStartUnbounded()) {
                    return -1;
                }
                /*
                 * sz Estimate = sz needed by underlying AggBuffer + sz for results + sz
                 * for maxChain + 3 * JavaDataModel.PRIMITIVES1 sz of results = sz of
                 * underlying * wdwSz sz of maxChain = sz of underlying * wdwSz
                 */

                int wdwSz = wFrameDef.getWindowSize();
                return underlying + (underlying * wdwSz) + (underlying * wdwSz)
                       + (3 * JavaDataModel.PRIMITIVES1);
            }

            @Override
            protected void reset() {
                super.reset();
                if (chain != null) {
                    chain.clear();
                }
                if (bitMap != null) {
                    bitMap.clear();
                }
            }

        }

        public ToBitMapStreamingFixedWindow(GenericUDAFEvaluator wrappedEval, WindowFrameDef wFrameDef) {
            super(wrappedEval, wFrameDef);
        }

        @Override public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            AggregationBuffer underlying = wrappedEval.getNewAggregationBuffer();
            return new State(underlying);
        }

        @Override public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
           State s = (State) agg;
           Object o = parameters[0];

            // We need to insert 'null' before processing first row for the case: X preceding and y preceding
            if (s.numRows == 0) {
                for (int i = wFrameDef.getEnd().getRelativeOffset(); i < 0; i++) {
                    s.results.add(null);
                }
            }
            if (!wFrameDef.isStartUnbounded()) {
                keepWindow(o,s);
            }
            wrappedEval.iterate(s.wrappedBuf, parameters);
            if (s.hasResultReady()) {
                RoaringBitmap bitmap = ((BitmapDataAggregationBuffer) s.wrappedBuf).bitmap;
                s.results.add(new BytesWritable(RoaringBitmapSerDe.serialize(bitmap).array()));
            }
            s.numRows++;
        }

        private void keepWindow(Object o, State s) {
            Deque<Object> chain = s.chain;
            Map<Object, Integer> bitMap = s.bitMap;
            if (wFrameDef.getWindowSize() == chain.size()) {
                Object o1 = chain.pollFirst();
                Integer integer = bitMap.get(o1);
                if (--integer==0){
                    bitMap.remove(o1);
                    Integer x = ((WritableIntObjectInspector) inputOI()).get(o1);
                    ((BitmapDataAggregationBuffer)s.wrappedBuf).bitmap.remove(x);
                }else{
                    bitMap.put(o1,integer);
                }
            }
            chain.addLast(o);
            bitMap.put(o,bitMap.getOrDefault(o,0)+1);
        }

        @Override public Object terminate(AggregationBuffer agg) throws HiveException {
            State s = (State) agg;
            Object o = wrappedEval.terminate(s.wrappedBuf);

            // After all the rows are processed, continue to generate results for the rows that results haven't generated.
            // For the case: X following and Y following, process first Y-X results and then insert X nulls.
            // For the case X preceding and Y following, process Y results.
            for (int i = Math.max(0, wFrameDef.getStart().getRelativeOffset()); i < wFrameDef.getEnd().getRelativeOffset(); i++) {
                if (s.hasResultReady()) {
                    if (!wFrameDef.isStartUnbounded()) {
                        keepWindow(o,s);
                    }
                    RoaringBitmap bitmap = ((BitmapDataAggregationBuffer) s.wrappedBuf).bitmap;
                    s.results.add(new BytesWritable(RoaringBitmapSerDe.serialize(bitmap).array()));
//                    s.results.add(getNextResult(s));
                }
                s.numRows++;
            }
            for (int i = 0; i < wFrameDef.getStart().getRelativeOffset(); i++) {
                if (s.hasResultReady()) {
                    s.results.add(null);
                }
                s.numRows++;
            }
            return o;
        }

        @Override public int getRowsRemainingAfterTerminate() throws HiveException {
            throw new UnsupportedOperationException();
        }
        protected ObjectInspector inputOI() {
            return ((UDAFToBitmap.RoaringBitmapEvaluator) wrappedEval).inputOI;
        }

        protected ObjectInspector outputOI() {
            return ((UDAFToBitmap.RoaringBitmapEvaluator) wrappedEval).outputOI;
        }
    }

}