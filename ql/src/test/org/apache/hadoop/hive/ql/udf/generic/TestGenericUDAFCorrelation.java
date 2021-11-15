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


import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

/**
 * TestGenericUDAFCorrelation.
 *
 */
public class TestGenericUDAFCorrelation {

  @Test
  public void testCorr() throws HiveException {
    GenericUDAFCorrelation corr = new GenericUDAFCorrelation();
    GenericUDAFEvaluator eval1 = corr.getEvaluator(
        new TypeInfo[]{TypeInfoFactory.doubleTypeInfo,TypeInfoFactory.doubleTypeInfo });
    GenericUDAFEvaluator eval2 = corr.getEvaluator(
        new TypeInfo[]{TypeInfoFactory.doubleTypeInfo,TypeInfoFactory.doubleTypeInfo });

    ObjectInspector poi1 = eval1.init(GenericUDAFEvaluator.Mode.PARTIAL1,
        new ObjectInspector[] {PrimitiveObjectInspectorFactory.javaDoubleObjectInspector,
            PrimitiveObjectInspectorFactory.javaDoubleObjectInspector});
    ObjectInspector poi2 = eval2.init(GenericUDAFEvaluator.Mode.PARTIAL1,
        new ObjectInspector[] {PrimitiveObjectInspectorFactory.javaDoubleObjectInspector,
            PrimitiveObjectInspectorFactory.javaDoubleObjectInspector});

    GenericUDAFEvaluator.AggregationBuffer buffer1 = eval1.getNewAggregationBuffer();
    eval1.iterate(buffer1, new Object[]{100d, 200d});
    eval1.iterate(buffer1, new Object[]{150d, 210d});
    eval1.iterate(buffer1, new Object[]{200d, 220d});
    Object object1 = eval1.terminatePartial(buffer1);

    GenericUDAFEvaluator.AggregationBuffer buffer2 = eval2.getNewAggregationBuffer();
    eval2.iterate(buffer2, new Object[]{250d, 230d});
    eval2.iterate(buffer2, new Object[]{250d, 240d});
    eval2.iterate(buffer2, new Object[]{300d, 250d});
    eval2.iterate(buffer2, new Object[]{350d, 260d});
    Object object2 = eval2.terminatePartial(buffer2);

    ObjectInspector coi = eval2.init(GenericUDAFEvaluator.Mode.FINAL,
        new ObjectInspector[]{poi1});

    GenericUDAFEvaluator.AggregationBuffer buffer3 = eval2.getNewAggregationBuffer();
    eval2.merge(buffer3, object1);
    eval2.merge(buffer3, object2);

    Object result = eval2.terminate(buffer3);
    assertEquals("0.987829161147262", String.valueOf(result));
  }
}
