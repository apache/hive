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

import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFMurmurHash;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.*;
import static org.junit.Assert.*;

public class TestUDFBitmap extends TestBitmapBase {

  private static final Logger LOG = LoggerFactory.getLogger(TestUDFBitmap.class.getName());

  @Test public void testUDFBitmap() throws HiveException {
    //1,2
    UDFBitmap udfBitmap = new UDFBitmap();
    udfBitmap.initialize(new ObjectInspector[] { writableIntObjectInspector, writableIntObjectInspector });
    Object evaluate = udfBitmap.evaluate(new GenericUDF.DeferredObject[] { new GenericUDF.DeferredJavaObject(
        new IntWritable(1)), new GenericUDF.DeferredJavaObject(new IntWritable(2)) });
    assertEquals(evaluate, getBitmapFromInts(1, 2));
    //-1,-2
    assertThrows(IndexOutOfBoundsException.class, () -> {
      udfBitmap.initialize(new ObjectInspector[] { writableIntObjectInspector, writableIntObjectInspector });
      Object evaluate1 = udfBitmap.evaluate(new GenericUDF.DeferredObject[] { new GenericUDF.DeferredJavaObject(
          new IntWritable(-1)), new GenericUDF.DeferredJavaObject(new IntWritable(-2)) });
      assertEquals(evaluate1, getBitmapFromInts(-1, -2));
    });
    //()=()
    udfBitmap.initialize(new ObjectInspector[] {});
    evaluate = udfBitmap.evaluate(new GenericUDF.DeferredObject[] {});
    assertEquals(evaluate, getBitmapFromInts());
    // params size =0
    // error params type
    testUDFParams(UDFArgumentException.class,
        new ObjectInspector[] { PrimitiveObjectInspectorFactory.writableStringObjectInspector, writableIntObjectInspector },
        new UDFBitmapAnd());
  }

  @Test public void testUDFBitmapAnd() throws HiveException {
    //(3,2147483647) & (1,2)==0
    testUDFResult(new UDFBitmapAnd(), Arrays.asList(new int[] { 3, 2147483647 }, new int[] { 1, 2 }),
        getBitmapFromInts());
    // () & () ==()
    testUDFResult(new UDFBitmapAnd(), Arrays.asList(new int[] {}, new int[] {}), getBitmapFromInts());
    // (1,2,3,2147483647) & (1,2,2147483647) & (1,2,5,6,2147483647)==(1,2,2147483647)
    testUDFResult(new UDFBitmapAnd(), Arrays.asList(new int[] { 1, 2, 3, 2147483647 }, new int[] { 1, 2, 2147483647 },
        new int[] { 1, 2, 5, 6, 2147483647 }), getBitmapFromInts(1, 2, 2147483647));
    // error param length ,params size =0
    testUDFParams(UDFArgumentLengthException.class, new ObjectInspector[] {}, new UDFBitmapAnd());
    // error params type
    testUDFParams(UDFArgumentException.class,
        new ObjectInspector[] { PrimitiveObjectInspectorFactory.writableStringObjectInspector, writableIntObjectInspector },
        new UDFBitmapAnd());
  }

  @Test public void testUDFBitmapOr() throws HiveException {
    //(3,2147483647) & (1,2)==(3,2147483647,1,2)
    testUDFResult(new UDFBitmapOr(), Arrays.asList(new int[] { 3, 2147483647 }, new int[] { 1, 2 }),
        getBitmapFromInts(3, 2147483647, 1, 2));
    // () & () ==()
    testUDFResult(new UDFBitmapOr(), Arrays.asList(new int[] {}, new int[] {}), getBitmapFromInts());
    // (1,2,3,2147483647) & (1,2,2147483647) & (1,2,5,6,2147483647)==(1,2,2147483647)
    testUDFResult(new UDFBitmapOr(), Arrays.asList(new int[] { 1, 2, 3, 2147483647 }, new int[] { 1, 2, 2147483647 },
        new int[] { 1, 2, 5, 6, 2147483647 }), getBitmapFromInts(1, 2, 3, 5, 6, 2147483647));
    //error params size
    testUDFParams(UDFArgumentLengthException.class, new ObjectInspector[] {}, new UDFBitmapOr());
    // error params type
    testUDFParams(UDFArgumentException.class,
        new ObjectInspector[] { PrimitiveObjectInspectorFactory.writableStringObjectInspector, writableIntObjectInspector },
        new UDFBitmapOr());
  }

  @Test public void testUDFBitmapAndNot() throws HiveException {
    //(1000,2000,1) & (1,2)==(1000,2000)
    testUDFResult(new UDFBitmapAndNot(), Arrays.asList(new int[] { 1000, 2000, 1 }, new int[] { 1, 2 }),
        getBitmapFromInts(1000, 2000));
    // () & () ==()
    testUDFResult(new UDFBitmapAndNot(), Arrays.asList(new int[] {}, new int[] {}), getBitmapFromInts());
    // (1,2,3,2147483647) & (1,2,2147483647) & (1,2,5,6,2147483647)==(3)
    testUDFResult(new UDFBitmapAndNot(),
        Arrays.asList(new int[] { 1, 2, 3, 2147483647 }, new int[] { 1, 2, 2147483647 },
            new int[] { 1, 2, 5, 6, 2147483647 }), getBitmapFromInts(3));
    // error param length ,params size =0
    testUDFParams(UDFArgumentLengthException.class, new ObjectInspector[] {}, new UDFBitmapAndNot());
    // error params type
    testUDFParams(UDFArgumentException.class,
        new ObjectInspector[] { PrimitiveObjectInspectorFactory.writableStringObjectInspector, writableIntObjectInspector },
        new UDFBitmapAndNot());
  }

  @Test public void testUDFBitmapOrNot() throws HiveException {
    //(2,3) & (2,3)==(2,3,0,1)
    testUDFResult(new UDFBitmapOrNot(), Arrays.asList(new int[] { 2, 3 }, new int[] { 2, 3 }),
        getBitmapFromInts(2, 3, 0, 1));
    //(10,20) & (1,3)==(10,20,0,2)
    testUDFResult(new UDFBitmapOrNot(), Arrays.asList(new int[] { 10, 20 }, new int[] { 1, 3 }),
        getBitmapFromInts(10, 20, 0, 2));
    //() & (10,20)==(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 11, 12, 13, 14, 15, 16, 17, 18, 19)
    testUDFResult(new UDFBitmapOrNot(), Arrays.asList(new int[] {}, new int[] { 20, 10 }),
        getBitmapFromInts(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 11, 12, 13, 14, 15, 16, 17, 18, 19));
    // (5,10) & (2,4) ==(5,10,0,1,3)
    testUDFResult(new UDFBitmapOrNot(), Arrays.asList(new int[] { 5, 10 }, new int[] { 2, 4 }),
        getBitmapFromInts(5, 10, 0, 1, 3));
    // () & () ==()
    testUDFResult(new UDFBitmapOrNot(), Arrays.asList(new int[] {}, new int[] {}), getBitmapFromInts());
    // (1,2,3,2147483647) & (1,2,2147483647) & (1,2,5,6,2147483647)== error ,Requested array size exceeds VM limit
    assertThrows(NegativeArraySizeException.class, () -> testUDFResult(new UDFBitmapOrNot(),
        Arrays.asList(new int[] { 1, 2, 3, 2147483647 }, new int[] { 1, 2, 2147483647 },
            new int[] { 1, 2, 5, 6, 2147483647 }), getBitmapFromInts(3)));
    // error param length ,params size =0
    testUDFParams(UDFArgumentLengthException.class, new ObjectInspector[] {}, new UDFBitmapOrNot());
    // error params type
    testUDFParams(UDFArgumentException.class,
        new ObjectInspector[] { PrimitiveObjectInspectorFactory.writableStringObjectInspector, writableIntObjectInspector },
        new UDFBitmapOrNot());
  }

  @Test public void testUDFBitmapXor() throws HiveException {
    //(2,3) & (2,3)==()
    testUDFResult(new UDFBitmapXor(), Arrays.asList(new int[] { 2, 3 }, new int[] { 2, 3 }), getBitmapFromInts());
    //(10,20) & (1,3)==(10,20,1,3)
    testUDFResult(new UDFBitmapXor(), Arrays.asList(new int[] { 10, 20 }, new int[] { 1, 3 }),
        getBitmapFromInts(10, 20, 1, 3));
    //() & (10,20)==(10,20)
    testUDFResult(new UDFBitmapXor(), Arrays.asList(new int[] {}, new int[] { 20, 10 }), getBitmapFromInts(10, 20));
    // (5,10) & (2,4) ==(5,10,2,4)
    testUDFResult(new UDFBitmapXor(), Arrays.asList(new int[] { 5, 10 }, new int[] { 2, 4 }),
        getBitmapFromInts(5, 10, 2, 4));
    // () & () ==()
    testUDFResult(new UDFBitmapXor(), Arrays.asList(new int[] {}, new int[] {}), getBitmapFromInts());
    // (1,2,3,2147483647) & (1,2,2147483647) & (1,2,5,6,2147483647)==(1,2,3,5,6,2147483647)
    testUDFResult(new UDFBitmapXor(), Arrays.asList(new int[] { 1, 2, 3, 2147483647 }, new int[] { 1, 2, 2147483647 },
        new int[] { 1, 2, 5, 6, 2147483647 }), getBitmapFromInts(1, 2, 3, 5, 6, 2147483647));
    // error param length ,params size =0
    testUDFParams(UDFArgumentLengthException.class, new ObjectInspector[] {}, new UDFBitmapXor());
    // error params type
    testUDFParams(UDFArgumentException.class,
        new ObjectInspector[] { PrimitiveObjectInspectorFactory.writableStringObjectInspector, writableIntObjectInspector },
        new UDFBitmapXor());
  }

  @Test public void testUDFBitmapAndCount() throws HiveException {
    //(3,2147483647) & (1,2)==0
    testUDFResult(new UDFBitmapAndCount(), Arrays.asList(new int[] { 3, 2147483647 }, new int[] { 1, 2 }),
        new LongWritable(0));
    // () & () ==()
    testUDFResult(new UDFBitmapAndCount(), Arrays.asList(new int[] {}, new int[] {}), new LongWritable(0));
    // (1,2,3,2147483647) & (1,2,2147483647) & (1,2,5,6,2147483647)==(1,2,2147483647)
    testUDFResult(new UDFBitmapAndCount(),
        Arrays.asList(new int[] { 1, 2, 3, 2147483647 }, new int[] { 1, 2, 2147483647 },
            new int[] { 1, 2, 5, 6, 2147483647 }), new LongWritable(3));
    // error param length ,params size =0
    testUDFParams(UDFArgumentLengthException.class, new ObjectInspector[] {}, new UDFBitmapAndCount());
    // error params type
    testUDFParams(UDFArgumentException.class,
        new ObjectInspector[] { PrimitiveObjectInspectorFactory.writableStringObjectInspector, writableIntObjectInspector },
        new UDFBitmapAndCount());
  }

  @Test public void testUDFBitmapOrCount() throws HiveException {
    //(3,2147483647) & (1,2)==(3,2147483647,1,2)
    testUDFResult(new UDFBitmapOrCount(), Arrays.asList(new int[] { 3, 2147483647 }, new int[] { 1, 2 }),
        new LongWritable(4));
    // () & () ==()
    testUDFResult(new UDFBitmapOrCount(), Arrays.asList(new int[] {}, new int[] {}), new LongWritable(0));
    // (1,2,3,2147483647) & (1,2,2147483647) & (1,2,5,6,2147483647)==(1,2,2147483647)
    testUDFResult(new UDFBitmapOrCount(),
        Arrays.asList(new int[] { 1, 2, 3, 2147483647 }, new int[] { 1, 2, 2147483647 },
            new int[] { 1, 2, 5, 6, 2147483647 }), new LongWritable(6));
    //error params size
    testUDFParams(UDFArgumentLengthException.class, new ObjectInspector[] {}, new UDFBitmapOrCount());
    // error params type
    testUDFParams(UDFArgumentException.class,
        new ObjectInspector[] { PrimitiveObjectInspectorFactory.writableStringObjectInspector, writableIntObjectInspector },
        new UDFBitmapOrCount());
  }

  @Test public void testUDFBitmapAndNotCount() throws HiveException {
    //(1000,2000,1) & (1,2)==(1000,2000)
    testUDFResult(new UDFBitmapAndNotCount(), Arrays.asList(new int[] { 1000, 2000, 1 }, new int[] { 1, 2 }),
        new LongWritable(2));
    // () & () ==()
    testUDFResult(new UDFBitmapAndNotCount(), Arrays.asList(new int[] {}, new int[] {}), new LongWritable(0));
    // (1,2,3,2147483647) & (1,2,2147483647) & (1,2,5,6,2147483647)==(3)
    testUDFResult(new UDFBitmapAndNotCount(),
        Arrays.asList(new int[] { 1, 2, 3, 2147483647 }, new int[] { 1, 2, 2147483647 },
            new int[] { 1, 2, 5, 6, 2147483647 }), new LongWritable(1));
    // error param length ,params size =0
    testUDFParams(UDFArgumentLengthException.class, new ObjectInspector[] {}, new UDFBitmapAndNotCount());
    // error params type
    testUDFParams(UDFArgumentException.class,
        new ObjectInspector[] { PrimitiveObjectInspectorFactory.writableStringObjectInspector, writableIntObjectInspector },
        new UDFBitmapAndNotCount());
  }

  @Test public void testUDFBitmapOrNotCount() throws HiveException {
    //(2,3) & (2,3)==(2,3,0,1)
    testUDFResult(new UDFBitmapOrNotCount(), Arrays.asList(new int[] { 2, 3 }, new int[] { 2, 3 }),
        new LongWritable(4));
    //(10,20) & (1,3)==(10,20,0,2)
    testUDFResult(new UDFBitmapOrNotCount(), Arrays.asList(new int[] { 10, 20 }, new int[] { 1, 3 }),
        new LongWritable(4));
    //() & (10,20)==(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 11, 12, 13, 14, 15, 16, 17, 18, 19)
    testUDFResult(new UDFBitmapOrNotCount(), Arrays.asList(new int[] {}, new int[] { 20, 10 }), new LongWritable(19));
    // (5,10) & (2,4) ==(5,10,0,1,3)
    testUDFResult(new UDFBitmapOrNotCount(), Arrays.asList(new int[] { 5, 10 }, new int[] { 2, 4 }),
        new LongWritable(5));
    // () & () ==()
    testUDFResult(new UDFBitmapOrNotCount(), Arrays.asList(new int[] {}, new int[] {}), new LongWritable(0));
    // (1,2,3,2147483647) & (1,2,2147483647) & (1,2,5,6,2147483647)== error ,Requested array size exceeds VM limit
    testUDFResult(new UDFBitmapOrNotCount(),
        Arrays.asList(new int[] { 1, 2, 3, 2147483647 }, new int[] { 1, 2, 2147483647 },
            new int[] { 1, 2, 5, 6, 2147483647 }), new LongWritable(2147483648l));
    // error param length ,params size =0
    testUDFParams(UDFArgumentLengthException.class, new ObjectInspector[] {}, new UDFBitmapOrNotCount());
    // error params type
    testUDFParams(UDFArgumentException.class,
        new ObjectInspector[] { PrimitiveObjectInspectorFactory.writableStringObjectInspector, writableIntObjectInspector },
        new UDFBitmapOrNotCount());
  }

  @Test public void testUDFBitmapXorCount() throws HiveException {
    //(2,3) & (2,3)==()
    testUDFResult(new UDFBitmapXorCount(), Arrays.asList(new int[] { 2, 3 }, new int[] { 2, 3 }), new LongWritable(0));
    //(10,20) & (1,3)==(10,20,1,3)
    testUDFResult(new UDFBitmapXorCount(), Arrays.asList(new int[] { 10, 20 }, new int[] { 1, 3 }),
        new LongWritable(4));
    //() & (10,20)==(10,20)
    testUDFResult(new UDFBitmapXorCount(), Arrays.asList(new int[] {}, new int[] { 20, 10 }), new LongWritable(2));
    // (5,10) & (2,4) ==(5,10,2,4)
    testUDFResult(new UDFBitmapXorCount(), Arrays.asList(new int[] { 5, 10 }, new int[] { 2, 4 }), new LongWritable(4));
    // () & () ==()
    testUDFResult(new UDFBitmapXorCount(), Arrays.asList(new int[] {}, new int[] {}), new LongWritable(0));
    // (1,2,3,2147483647) & (1,2,2147483647) & (1,2,5,6,2147483647)==(6)
    testUDFResult(new UDFBitmapXorCount(),
        Arrays.asList(new int[] { 1, 2, 3, 2147483647 }, new int[] { 1, 2, 2147483647 },
            new int[] { 1, 2, 5, 6, 2147483647 }), new LongWritable(6));
    // error param length ,params size =0
    testUDFParams(UDFArgumentLengthException.class, new ObjectInspector[] {}, new UDFBitmapXorCount());
    // error params type
    testUDFParams(UDFArgumentException.class,
        new ObjectInspector[] { PrimitiveObjectInspectorFactory.writableStringObjectInspector, writableIntObjectInspector },
        new UDFBitmapXorCount());
  }

  @Test public void testUDFBitmapToString() throws HiveException {
    //(1,2)="1,2"
    testUDFResult(new UDFBitmapToString(), Arrays.asList(new int[] { 1, 2 }), new Text("1,2"));
    //( 3)="3"
    testUDFResult(new UDFBitmapToString(), Arrays.asList(new int[] { 3 }), new Text("3"));
    //()==""
    testUDFResult(new UDFBitmapToString(), Arrays.asList(new int[] {}), new Text(""));
    // (1,2,3,2147483647)="1,2,3,2147483647"
    testUDFResult(new UDFBitmapToString(), Arrays.asList(new int[] { 1, 2, 3, 2147483647 }),
        new Text("1,2,3,2147483647"));
    // error param length ,params size =0
    testUDFParams(UDFArgumentLengthException.class, new ObjectInspector[] {}, new UDFBitmapToString());
    // error param length ,params size >1
    testUDFParams(UDFArgumentLengthException.class,
        new ObjectInspector[] { PrimitiveObjectInspectorFactory.writableBinaryObjectInspector, PrimitiveObjectInspectorFactory.writableBinaryObjectInspector },
        new UDFBitmapToString());
    // error params type
    testUDFParams(UDFArgumentException.class,
        new ObjectInspector[] { PrimitiveObjectInspectorFactory.writableStringObjectInspector, writableIntObjectInspector },
        new UDFBitmapToString());
  }

  @Test public void testUDFBitmapToArray() throws HiveException {
    //(1,2)=[1,2]
    testUDFArrayResult(new UDFBitmapToArray(), Arrays.asList(new int[] { 1, 2 }), new int[] { 1, 2 });
    //( 1,3)=[1,3]
    testUDFArrayResult(new UDFBitmapToArray(), Arrays.asList(new int[] { 1, 3 }), new int[] { 1, 3 });
    // ()==""
    testUDFArrayResult(new UDFBitmapToArray(), Arrays.asList(new int[] {}), new int[] {});
    // (1,2,3,2147483647)="1,2,3,2147483647"
    testUDFArrayResult(new UDFBitmapToArray(), Arrays.asList(new int[] { 1, 2, 3, 2147483647 }),
        new int[] { 1, 2, 3, 2147483647 });
    // error param length ,params size =0
    testUDFParams(UDFArgumentLengthException.class, new ObjectInspector[] {}, new UDFBitmapToArray());
    // error param length ,params size >1
    testUDFParams(UDFArgumentLengthException.class,
        new ObjectInspector[] { PrimitiveObjectInspectorFactory.writableBinaryObjectInspector, PrimitiveObjectInspectorFactory.writableBinaryObjectInspector },
        new UDFBitmapToArray());
    // error params type
    testUDFParams(UDFArgumentException.class,
        new ObjectInspector[] { PrimitiveObjectInspectorFactory.writableStringObjectInspector, writableIntObjectInspector },
        new UDFBitmapToArray());
  }

  @Test public void testUDFBitmapCount() throws HiveException {
    //(1,2)=2
    testUDFResult(new UDFBitmapCount(), Arrays.asList(new int[] { 1, 2 }), new LongWritable(2));
    //(1,1,2,2147483647)=3
    testUDFResult(new UDFBitmapCount(), Arrays.asList(new int[] { 1, 1, 2, 2147483647 }), new LongWritable(3));
    // ()==0
    testUDFResult(new UDFBitmapCount(), Arrays.asList(new int[] {}), new LongWritable(0));
    // (1,2,3,2147483647)=4
    testUDFResult(new UDFBitmapCount(), Arrays.asList(new int[] { 1, 2, 3, 2147483647 }), new LongWritable(4));
    // error param length ,params size =0
    testUDFParams(UDFArgumentLengthException.class, new ObjectInspector[] {}, new UDFBitmapCount());
    // error param length ,params size >1
    testUDFParams(UDFArgumentLengthException.class,
        new ObjectInspector[] { PrimitiveObjectInspectorFactory.writableBinaryObjectInspector, PrimitiveObjectInspectorFactory.writableBinaryObjectInspector },
        new UDFBitmapCount());
    // error params type
    testUDFParams(UDFArgumentException.class,
        new ObjectInspector[] { PrimitiveObjectInspectorFactory.writableStringObjectInspector, writableIntObjectInspector },
        new UDFBitmapCount());
  }

  @Test public void testUDFBitmapEmpty() throws HiveException {
    //()=()
    testUDFResult(new UDFBitmapEmpty(), Arrays.asList(), getBitmapFromInts());
    // error param length ,params size >0
    testUDFParams(UDFArgumentLengthException.class,
        new ObjectInspector[] { PrimitiveObjectInspectorFactory.writableBinaryObjectInspector, PrimitiveObjectInspectorFactory.writableBinaryObjectInspector },
        new UDFBitmapEmpty());
    // error params type
    testUDFParams(UDFArgumentException.class,
        new ObjectInspector[] { PrimitiveObjectInspectorFactory.writableStringObjectInspector, writableIntObjectInspector },
        new UDFBitmapEmpty());
  }

  @Test public void testUDFBitmapHasAny() throws HiveException {
    //() 、()=true
    testUDFResult(new UDFBitmapHasAny(), Arrays.asList(new int[] {}, new int[] {}), new BooleanWritable(true));
    //(0) 、(1)=false
    testUDFResult(new UDFBitmapHasAny(), Arrays.asList(new int[] { 0 }, new int[] { 1 }), new BooleanWritable(false));
    //(13,15) 、(13)=true
    testUDFResult(new UDFBitmapHasAny(), Arrays.asList(new int[] { 13, 15 }, new int[] { 13 }),
        new BooleanWritable(true));
    // error param length ,params size !=2
    testUDFParams(UDFArgumentLengthException.class,
        new ObjectInspector[] { PrimitiveObjectInspectorFactory.writableBinaryObjectInspector }, new UDFBitmapHasAny());
    // error params type
    testUDFParams(UDFArgumentException.class,
        new ObjectInspector[] { PrimitiveObjectInspectorFactory.writableStringObjectInspector, writableIntObjectInspector },
        new UDFBitmapHasAny());
  }

  @Test public void testUDFBitmapHasAll() throws HiveException {
    //() 、()=true
    testUDFResult(new UDFBitmapHasAll(), Arrays.asList(new int[] {}, new int[] {}), new BooleanWritable(true));
    //() 、(1)=false
    testUDFResult(new UDFBitmapHasAll(), Arrays.asList(new int[] {}, new int[] { 1 }), new BooleanWritable(false));
    //(13,15,100) 、(13,15)=true
    testUDFResult(new UDFBitmapHasAll(), Arrays.asList(new int[] { 13, 15, 100 }, new int[] { 13, 15 }),
        new BooleanWritable(true));
    // error param length ,params size !=2
    testUDFParams(UDFArgumentLengthException.class,
        new ObjectInspector[] { PrimitiveObjectInspectorFactory.writableBinaryObjectInspector }, new UDFBitmapHasAll());
    // error params type
    testUDFParams(UDFArgumentException.class,
        new ObjectInspector[] { PrimitiveObjectInspectorFactory.writableStringObjectInspector, writableIntObjectInspector },
        new UDFBitmapHasAll());
  }

  @Test public void testUDFBitmapMin() throws HiveException {
    //(0,1,2,3) =0
    testUDFResult(new UDFBitmapMin(), Arrays.asList(new int[] { 0, 1, 2, 3 }), 0);
    //() = null
    testUDFResult(new UDFBitmapMin(), Arrays.asList(new int[] {}), null);
    //(1,2,3) =1
    testUDFResult(new UDFBitmapMin(), Arrays.asList(new int[] { 1, 2, 3 }), 1);
    // error param length ,params size !=1
    testUDFParams(UDFArgumentLengthException.class,
        new ObjectInspector[] { PrimitiveObjectInspectorFactory.writableBinaryObjectInspector, PrimitiveObjectInspectorFactory.writableBinaryObjectInspector },
        new UDFBitmapMin());
    // error params type
    testUDFParams(UDFArgumentException.class,
        new ObjectInspector[] { PrimitiveObjectInspectorFactory.writableStringObjectInspector }, new UDFBitmapMin());
  }

  @Test public void testUDFBitmapMax() throws HiveException {
    //0,1,2,3) =3
    testUDFResult(new UDFBitmapMax(), Arrays.asList(new int[] { 0, 1, 2, 3 }), 3);
    //(1,0) =1
    testUDFResult(new UDFBitmapMax(), Arrays.asList(new int[] { 1, 0 }), 1);
    //() = null
    testUDFResult(new UDFBitmapMax(), Arrays.asList(new int[] {}), null);
    // error param length ,params size !=1
    testUDFParams(UDFArgumentLengthException.class,
        new ObjectInspector[] { PrimitiveObjectInspectorFactory.writableBinaryObjectInspector, PrimitiveObjectInspectorFactory.writableBinaryObjectInspector },
        new UDFBitmapMax());
    // error params type
    testUDFParams(UDFArgumentException.class,
        new ObjectInspector[] { PrimitiveObjectInspectorFactory.writableStringObjectInspector }, new UDFBitmapMax());
  }

  @Test public void testUDFBitmapSub() throws HiveException {
    //(0,1,2,3,4), 0,4 =(0,1,2,3)
    UDFBitmapSub udfBitmapSub = new UDFBitmapSub();
    udfBitmapSub.initialize(
        new ObjectInspector[] { PrimitiveObjectInspectorFactory.writableBinaryObjectInspector, writableIntObjectInspector, writableIntObjectInspector });
    Object evaluate = udfBitmapSub.evaluate(new GenericUDF.DeferredObject[] { new GenericUDF.DeferredJavaObject(
        getBitmapFromInts(0, 1, 2, 3, 4)), new GenericUDF.DeferredJavaObject(
        new IntWritable(0)), new GenericUDF.DeferredJavaObject(new IntWritable(4)) });
    assertEquals(getBitmapFromInts(0, 1, 2, 3), evaluate);
    //(0,1,2,3), 200,4 =()
    udfBitmapSub.initialize(
        new ObjectInspector[] { PrimitiveObjectInspectorFactory.writableBinaryObjectInspector, writableIntObjectInspector, writableIntObjectInspector });
    evaluate = udfBitmapSub.evaluate(new GenericUDF.DeferredObject[] { new GenericUDF.DeferredJavaObject(
        getBitmapFromInts(0, 1, 2, 3)), new GenericUDF.DeferredJavaObject(
        new IntWritable(200)), new GenericUDF.DeferredJavaObject(new IntWritable(4)) });
    assertEquals(getBitmapFromInts(), evaluate);
    //(2147483647), 2147483647,2147483647
    udfBitmapSub.initialize(
        new ObjectInspector[] { PrimitiveObjectInspectorFactory.writableBinaryObjectInspector, writableLongObjectInspector, writableLongObjectInspector });
    evaluate = udfBitmapSub.evaluate(new GenericUDF.DeferredObject[] { new GenericUDF.DeferredJavaObject(
        getBitmapFromInts(Integer.MAX_VALUE)), new GenericUDF.DeferredJavaObject(
        new LongWritable(Integer.MAX_VALUE)), new GenericUDF.DeferredJavaObject(new LongWritable(Integer.MAX_VALUE)) });
    assertEquals(getBitmapFromInts(), evaluate);
    //(0,1,2,3), -1,4 =IndexOutOfBoundsException
    assertThrows(IndexOutOfBoundsException.class, () -> {
      udfBitmapSub.evaluate(new GenericUDF.DeferredObject[] { new GenericUDF.DeferredJavaObject(
          getBitmapFromInts(-212121, 0, 1, 2, 3)), new GenericUDF.DeferredJavaObject(
          new IntWritable(0)), new GenericUDF.DeferredJavaObject(new IntWritable(4)) });
    });
    //(0,1,2,3), 0 =(0,1,2,3)
    udfBitmapSub.initialize(
        new ObjectInspector[] { PrimitiveObjectInspectorFactory.writableBinaryObjectInspector, writableIntObjectInspector });
    evaluate = udfBitmapSub.evaluate(new GenericUDF.DeferredObject[] { new GenericUDF.DeferredJavaObject(
        getBitmapFromInts(0, 1, 2, 3)), new GenericUDF.DeferredJavaObject(new IntWritable(0)) });
    assertEquals(getBitmapFromInts(0, 1, 2, 3), evaluate);
    // error param length ,params size !=3 && !=2
    testUDFParams(UDFArgumentLengthException.class,
        new ObjectInspector[] { PrimitiveObjectInspectorFactory.writableBinaryObjectInspector }, new UDFBitmapSub());
    // error params type
    testUDFParams(UDFArgumentException.class,
        new ObjectInspector[] { PrimitiveObjectInspectorFactory.writableStringObjectInspector }, new UDFBitmapSub());
  }

  @Test public void testUDFBitmapHash() throws HiveException {
    //'i am cscs' =(0,1,2,3)
    Text str = new Text();
    str.set("i am cscs");
    UDFBitmapHash udfBitmapHash = new UDFBitmapHash();
    udfBitmapHash.initialize(new ObjectInspector[] { PrimitiveObjectInspectorFactory.writableStringObjectInspector });
    Object result = udfBitmapHash.evaluate(new GenericUDF.DeferredObject[] { new GenericUDF.DeferredJavaObject(str) });
    GenericUDFMurmurHash hash = new GenericUDFMurmurHash();
    hash.initialize(new ObjectInspector[] { PrimitiveObjectInspectorFactory.writableStringObjectInspector });
    int hashCode = ((IntWritable) hash.evaluate(
        new GenericUDF.DeferredObject[] { new GenericUDF.DeferredJavaObject(str) })).get();
    assertEquals(getBitmapFromInts(hashCode), result);
  }

  @Test public void testUDFBitmapContains() throws HiveException {
    //(0,1,2,3) , 100=false
    UDFBitmapContains udfBitmapContains = new UDFBitmapContains();
    udfBitmapContains.initialize(
        new ObjectInspector[] { PrimitiveObjectInspectorFactory.writableBinaryObjectInspector, writableIntObjectInspector });
    Object evaluate = udfBitmapContains.evaluate(new GenericUDF.DeferredObject[] { new GenericUDF.DeferredJavaObject(
        getBitmapFromInts(0, 1, 2, 3)), new GenericUDF.DeferredJavaObject(new IntWritable(100)) });
    assertEquals(new BooleanWritable(false), evaluate);
    //(0,1,2,3) , 2=true
    evaluate = udfBitmapContains.evaluate(new GenericUDF.DeferredObject[] { new GenericUDF.DeferredJavaObject(
        getBitmapFromInts(0, 1, 2, 3)), new GenericUDF.DeferredJavaObject(new IntWritable(2)) });
    assertEquals(new BooleanWritable(true), evaluate);
    // error param length ,params size !=2
    testUDFParams(UDFArgumentLengthException.class,
        new ObjectInspector[] { PrimitiveObjectInspectorFactory.writableBinaryObjectInspector, PrimitiveObjectInspectorFactory.writableBinaryObjectInspector, PrimitiveObjectInspectorFactory.writableBinaryObjectInspector },
        new UDFBitmapContains());
    // error params type
    testUDFParams(UDFArgumentException.class,
        new ObjectInspector[] { PrimitiveObjectInspectorFactory.writableBinaryObjectInspector, PrimitiveObjectInspectorFactory.writableBinaryObjectInspector },
        new UDFBitmapContains());
  }

  @Test public void testUDFBitmapFromString() throws HiveException {
    //"0,1,2,3"=(-212121,0,1,2,3)
    UDFBitmapFromString udfBitmapFromString = new UDFBitmapFromString();
    udfBitmapFromString.initialize(
        new ObjectInspector[] { PrimitiveObjectInspectorFactory.writableStringObjectInspector });
    Object evaluate = udfBitmapFromString.evaluate(
        new GenericUDF.DeferredObject[] { new GenericUDF.DeferredJavaObject("0,1,2,3") });
    assertEquals(getBitmapFromInts(0, 1, 2, 3), evaluate);
    //""=()
    evaluate = udfBitmapFromString.evaluate(new GenericUDF.DeferredObject[] { new GenericUDF.DeferredJavaObject("") });
    assertEquals(getBitmapFromInts(), evaluate);
    assertThrows(NumberFormatException.class, () -> {
      udfBitmapFromString.evaluate(new GenericUDF.DeferredObject[] { new GenericUDF.DeferredJavaObject("123.5,6") });
    });
    // error param length ,params size !=1
    testUDFParams(UDFArgumentLengthException.class,
        new ObjectInspector[] { PrimitiveObjectInspectorFactory.writableBinaryObjectInspector, PrimitiveObjectInspectorFactory.writableStringObjectInspector, PrimitiveObjectInspectorFactory.writableStringObjectInspector },
        new UDFBitmapFromString());
  }

  @Test public void testUDFBitmapFromArray() throws HiveException {
    //[0,1,2,3]=(0,1,2,3)
    UDFBitmapFromArray udfBitmapFromArray = new UDFBitmapFromArray();
    udfBitmapFromArray.initialize(new ObjectInspector[] { ObjectInspectorFactory.getStandardListObjectInspector(
        PrimitiveObjectInspectorFactory.writableIntObjectInspector) });
    Object evaluate = udfBitmapFromArray.evaluate(new GenericUDF.DeferredObject[] { new GenericUDF.DeferredJavaObject(
        Arrays.asList(new IntWritable(0), new IntWritable(1), new IntWritable(2), new IntWritable(3))) });
    assertEquals(getBitmapFromInts(0, 1, 2, 3), evaluate);
    //[]=()
    evaluate = udfBitmapFromArray.evaluate(
        new GenericUDF.DeferredObject[] { new GenericUDF.DeferredJavaObject(Arrays.asList()) });
    assertEquals(getBitmapFromInts(), evaluate);
    // error param length ,params size !=1
    testUDFParams(UDFArgumentLengthException.class,
        new ObjectInspector[] { PrimitiveObjectInspectorFactory.writableBinaryObjectInspector, PrimitiveObjectInspectorFactory.writableStringObjectInspector, PrimitiveObjectInspectorFactory.writableStringObjectInspector },
        new UDFBitmapFromArray());
    // error params type
    testUDFParams(UDFArgumentException.class,
        new ObjectInspector[] { ObjectInspectorFactory.getStandardListObjectInspector(
            PrimitiveObjectInspectorFactory.writableLongObjectInspector) }, new UDFBitmapFromArray());
    testUDFParams(UDFArgumentException.class,
        new ObjectInspector[] { PrimitiveObjectInspectorFactory.writableLongObjectInspector },
        new UDFBitmapFromArray());
  }

  @Test public void testUDFBitmapRange() throws HiveException {
    //1,3=(1,2)
    UDFBitmapRange udfBitmapRange = new UDFBitmapRange();
    udfBitmapRange.initialize(new ObjectInspector[] { writableLongObjectInspector, writableLongObjectInspector });
    Object evaluate = udfBitmapRange.evaluate(new GenericUDF.DeferredObject[] { new GenericUDF.DeferredJavaObject(
        new LongWritable(1)), new GenericUDF.DeferredJavaObject(new LongWritable(3)) });
    assertEquals(evaluate, getBitmapFromInts(1, 2));
    //2147483647,2147483648=(2147483647)
    udfBitmapRange.initialize(new ObjectInspector[] { writableLongObjectInspector, writableLongObjectInspector });
    evaluate = udfBitmapRange.evaluate(new GenericUDF.DeferredObject[] { new GenericUDF.DeferredJavaObject(
        new LongWritable(2147483647)), new GenericUDF.DeferredJavaObject(new LongWritable(2147483648l)) });
    assertEquals(evaluate, getBitmapFromInts(2147483647));
    //0,6=(0,1,2,3,4,5)
    evaluate = udfBitmapRange.evaluate(new GenericUDF.DeferredObject[] { new GenericUDF.DeferredJavaObject(
        new LongWritable(0)), new GenericUDF.DeferredJavaObject(new LongWritable(6)) });
    assertEquals(evaluate, getBitmapFromInts(0, 1, 2, 3, 4, 5));
    //6,6=0
    evaluate = udfBitmapRange.evaluate(new GenericUDF.DeferredObject[] { new GenericUDF.DeferredJavaObject(
        new LongWritable(6)), new GenericUDF.DeferredJavaObject(new LongWritable(6)) });
    assertEquals(evaluate, getBitmapFromInts());
    // error param length ,params size !=2
    testUDFParams(UDFArgumentException.class, new ObjectInspector[] { writableLongObjectInspector },
        new UDFBitmapRange());
    // params size =0
    // error params type
    testUDFParams(UDFArgumentException.class,
        new ObjectInspector[] { writableBinaryObjectInspector, writableIntObjectInspector }, new UDFBitmapRange());
  }

  @Test public void testUDFBitmapSelect() throws HiveException {
    //(1,3) , 0 =1
    UDFBitmapSelect udfBitmapSelect = new UDFBitmapSelect();
    udfBitmapSelect.initialize(new ObjectInspector[] { writableBinaryObjectInspector, writableIntObjectInspector });
    Object evaluate = udfBitmapSelect.evaluate(new GenericUDF.DeferredObject[] { new GenericUDF.DeferredJavaObject(
        getBitmapFromInts(1, 3)), new GenericUDF.DeferredJavaObject(new IntWritable(0)) });
    assertEquals(evaluate, new IntWritable(1));
    //(2147483646,2147483647) ,1 =(2147483647)
    evaluate = udfBitmapSelect.evaluate(new GenericUDF.DeferredObject[] { new GenericUDF.DeferredJavaObject(
        getBitmapFromInts(2147483646, 2147483647)), new GenericUDF.DeferredJavaObject(new IntWritable(1)) });
    assertEquals(evaluate, new IntWritable(2147483647));
    //(0,1,2,3,4,5) , 1 =1
    evaluate = udfBitmapSelect.evaluate(new GenericUDF.DeferredObject[] { new GenericUDF.DeferredJavaObject(
        getBitmapFromInts(0, 1, 2, 3, 4, 5)), new GenericUDF.DeferredJavaObject(new IntWritable(1)) });
    assertEquals(evaluate, new IntWritable(1));
    //(6),6=null
    evaluate = udfBitmapSelect.evaluate(new GenericUDF.DeferredObject[] { new GenericUDF.DeferredJavaObject(
        getBitmapFromInts(6)), new GenericUDF.DeferredJavaObject(new IntWritable(6)) });
    assertEquals(evaluate, null);
    //(),6=null
    evaluate = udfBitmapSelect.evaluate(new GenericUDF.DeferredObject[] { new GenericUDF.DeferredJavaObject(
        getBitmapFromInts()), new GenericUDF.DeferredJavaObject(new IntWritable(6)) });
    assertEquals(evaluate, null);
    // error param length ,params size !=2
    testUDFParams(UDFArgumentException.class, new ObjectInspector[] { writableBinaryObjectInspector },
        new UDFBitmapSelect());
    // params size =0
    // error params type
    testUDFParams(UDFArgumentException.class,
        new ObjectInspector[] { writableIntObjectInspector, writableIntObjectInspector }, new UDFBitmapSelect());
  }

  @Test public void testUDFBitmapRank() throws HiveException {
    //(1,3) , 0 =0
    UDFBitmapRank udfBitmapRank = new UDFBitmapRank();
    udfBitmapRank.initialize(new ObjectInspector[] { writableBinaryObjectInspector, writableIntObjectInspector });
    Object evaluate = udfBitmapRank.evaluate(new GenericUDF.DeferredObject[] { new GenericUDF.DeferredJavaObject(
        getBitmapFromInts(1, 3)), new GenericUDF.DeferredJavaObject(new IntWritable(0)) });
    assertEquals(evaluate, new LongWritable(0));
    //(1,2,3,4,5,6,7) ,4 =4
    evaluate = udfBitmapRank.evaluate(new GenericUDF.DeferredObject[] { new GenericUDF.DeferredJavaObject(
        getBitmapFromInts(1, 2, 3, 4, 5, 6, 7)), new GenericUDF.DeferredJavaObject(new IntWritable(4)) });
    assertEquals(evaluate, new LongWritable(4));
    //(0,1,2,3,4,5) , 1 =2
    evaluate = udfBitmapRank.evaluate(new GenericUDF.DeferredObject[] { new GenericUDF.DeferredJavaObject(
        getBitmapFromInts(0, 1, 2, 3, 4, 5)), new GenericUDF.DeferredJavaObject(new IntWritable(1)) });
    assertEquals(evaluate, new LongWritable(2));
    //(6),6=1
    evaluate = udfBitmapRank.evaluate(new GenericUDF.DeferredObject[] { new GenericUDF.DeferredJavaObject(
        getBitmapFromInts(6)), new GenericUDF.DeferredJavaObject(new IntWritable(6)) });
    assertEquals(evaluate, new LongWritable(1));
    //(),6=0
    evaluate = udfBitmapRank.evaluate(new GenericUDF.DeferredObject[] { new GenericUDF.DeferredJavaObject(
        getBitmapFromInts()), new GenericUDF.DeferredJavaObject(new IntWritable(6)) });
    assertEquals(evaluate, new LongWritable(0));
    // error param length ,params size !=2
    testUDFParams(UDFArgumentException.class, new ObjectInspector[] { writableBinaryObjectInspector },
        new UDFBitmapRank());
    // params size =0
    // error params type
    testUDFParams(UDFArgumentException.class,
        new ObjectInspector[] { writableIntObjectInspector, writableIntObjectInspector }, new UDFBitmapRank());
  }

  @Test public void testFunctionsRegistry() throws SemanticException {
    assertEquals(UDAFToBitmap.FUNC_NAME,FunctionRegistry.getFunctionInfo(UDAFToBitmap.FUNC_NAME).getDisplayName());
    assertEquals(UDAFBitmapIntersect.FUNC_NAME,FunctionRegistry.getFunctionInfo(UDAFBitmapIntersect.FUNC_NAME).getDisplayName());
    assertEquals(UDAFBitmapUnion.FUNC_NAME,FunctionRegistry.getFunctionInfo(UDAFBitmapUnion.FUNC_NAME).getDisplayName());
    assertEquals(UDFBitmap.FUNC_NAME,FunctionRegistry.getFunctionInfo(UDFBitmap.FUNC_NAME).getDisplayName());
    assertEquals(UDFBitmapAnd.FUNC_NAME,FunctionRegistry.getFunctionInfo(UDFBitmapAnd.FUNC_NAME).getDisplayName());
    assertEquals(UDFBitmapAndCount.FUNC_NAME,FunctionRegistry.getFunctionInfo(UDFBitmapAndCount.FUNC_NAME).getDisplayName());
    assertEquals(UDFBitmapAndNot.FUNC_NAME,FunctionRegistry.getFunctionInfo(UDFBitmapAndNot.FUNC_NAME).getDisplayName());
    assertEquals(UDFBitmapAndNotCount.FUNC_NAME,FunctionRegistry.getFunctionInfo(UDFBitmapAndNotCount.FUNC_NAME).getDisplayName());
    assertEquals(UDFBitmapContains.FUNC_NAME,FunctionRegistry.getFunctionInfo(UDFBitmapContains.FUNC_NAME).getDisplayName());
    assertEquals(UDFBitmapCount.FUNC_NAME,FunctionRegistry.getFunctionInfo(UDFBitmapCount.FUNC_NAME).getDisplayName());
    assertEquals(UDFBitmapEmpty.FUNC_NAME,FunctionRegistry.getFunctionInfo(UDFBitmapEmpty.FUNC_NAME).getDisplayName());
    assertEquals(UDFBitmapFromArray.FUNC_NAME,FunctionRegistry.getFunctionInfo(UDFBitmapFromArray.FUNC_NAME).getDisplayName());
    assertEquals(UDFBitmapFromString.FUNC_NAME,FunctionRegistry.getFunctionInfo(UDFBitmapFromString.FUNC_NAME).getDisplayName());
    assertEquals(UDFBitmapHasAll.FUNC_NAME,FunctionRegistry.getFunctionInfo(UDFBitmapHasAll.FUNC_NAME).getDisplayName());
    assertEquals(UDFBitmapHasAny.FUNC_NAME,FunctionRegistry.getFunctionInfo(UDFBitmapHasAny.FUNC_NAME).getDisplayName());
    assertEquals(UDFBitmapHash.FUNC_NAME,FunctionRegistry.getFunctionInfo(UDFBitmapHash.FUNC_NAME).getDisplayName());
    assertEquals(UDFBitmapMax.FUNC_NAME,FunctionRegistry.getFunctionInfo(UDFBitmapMax.FUNC_NAME).getDisplayName());
    assertEquals(UDFBitmapMin.FUNC_NAME,FunctionRegistry.getFunctionInfo(UDFBitmapMin.FUNC_NAME).getDisplayName());
    assertEquals(UDFBitmapOrNot.FUNC_NAME,FunctionRegistry.getFunctionInfo(UDFBitmapOrNot.FUNC_NAME).getDisplayName());
    assertEquals(UDFBitmapOrNotCount.FUNC_NAME,FunctionRegistry.getFunctionInfo(UDFBitmapOrNotCount.FUNC_NAME).getDisplayName());
    assertEquals(UDFBitmapRange.FUNC_NAME,FunctionRegistry.getFunctionInfo(UDFBitmapRange.FUNC_NAME).getDisplayName());
    assertEquals(UDFBitmapRank.FUNC_NAME,FunctionRegistry.getFunctionInfo(UDFBitmapRank.FUNC_NAME).getDisplayName());
    assertEquals(UDFBitmapSelect.FUNC_NAME,FunctionRegistry.getFunctionInfo(UDFBitmapSelect.FUNC_NAME).getDisplayName());
    assertEquals(UDFBitmapOr.FUNC_NAME,FunctionRegistry.getFunctionInfo(UDFBitmapOr.FUNC_NAME).getDisplayName());
    assertEquals(UDFBitmapOrCount.FUNC_NAME,FunctionRegistry.getFunctionInfo(UDFBitmapOrCount.FUNC_NAME).getDisplayName());
    assertEquals(UDFBitmapSub.FUNC_NAME,FunctionRegistry.getFunctionInfo(UDFBitmapSub.FUNC_NAME).getDisplayName());
    assertEquals(UDFBitmapToArray.FUNC_NAME,FunctionRegistry.getFunctionInfo(UDFBitmapToArray.FUNC_NAME).getDisplayName());
    assertEquals(UDFBitmapToString.FUNC_NAME,FunctionRegistry.getFunctionInfo(UDFBitmapToString.FUNC_NAME).getDisplayName());
    assertEquals(UDFBitmapXor.FUNC_NAME,FunctionRegistry.getFunctionInfo(UDFBitmapXor.FUNC_NAME).getDisplayName());
    assertEquals(UDFBitmapXorCount.FUNC_NAME,FunctionRegistry.getFunctionInfo(UDFBitmapXorCount.FUNC_NAME).getDisplayName());
    assertEquals(UDFBitmapToString.FUNC_NAME,FunctionRegistry.getFunctionInfo(UDFBitmapToString.FUNC_NAME).getDisplayName());
  }

  private void testUDFParams(Class e, ObjectInspector[] params, GenericUDF bitmapUDF) throws HiveException {
    assertThrows(e, () -> {
      bitmapUDF.initialize(params);
    });
  }

  private void testUDFResult(GenericUDF bitmapUDF, List<int[]> params, Object expectResult) throws HiveException {
    ObjectInspector[] arguments = new ObjectInspector[params.size()];
    Arrays.fill(arguments, PrimitiveObjectInspectorFactory.writableBinaryObjectInspector);
    bitmapUDF.initialize(arguments);
    List<GenericUDF.DeferredJavaObject> collect = params.stream()
        .map(ints -> new GenericUDF.DeferredJavaObject(getBitmapFromInts(ints))).collect(Collectors.toList());
    GenericUDF.DeferredObject[] deferredObjects = new GenericUDF.DeferredObject[0];
    GenericUDF.DeferredObject[] params1 = collect.toArray(deferredObjects);
    Object evaluate = bitmapUDF.evaluate(params1);
    assertEquals(evaluate, expectResult);
  }

  private void testUDFArrayResult(GenericUDF bitmapUDF, List<int[]> params, Object expectResult) throws HiveException {
    ObjectInspector[] arguments = new ObjectInspector[params.size()];
    Arrays.fill(arguments, PrimitiveObjectInspectorFactory.writableBinaryObjectInspector);
    bitmapUDF.initialize(arguments);
    List<GenericUDF.DeferredJavaObject> collect = params.stream()
        .map(ints -> new GenericUDF.DeferredJavaObject(getBitmapFromInts(ints))).collect(Collectors.toList());
    GenericUDF.DeferredObject[] deferredObjects = new GenericUDF.DeferredObject[0];
    GenericUDF.DeferredObject[] params1 = collect.toArray(deferredObjects);
    Object evaluate = bitmapUDF.evaluate(params1);
    assertArrayEquals((int[]) evaluate, (int[]) expectResult);
  }
}