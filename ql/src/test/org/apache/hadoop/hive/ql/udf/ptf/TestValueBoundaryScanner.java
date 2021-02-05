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
package org.apache.hadoop.hive.ql.udf.ptf;

import java.time.ZoneId;

import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.common.type.TimestampTZ;
import org.apache.hadoop.hive.ql.plan.ptf.OrderExpressionDef;
import org.apache.hadoop.hive.ql.plan.ptf.PTFExpressionDef;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.TimestampLocalTZWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

public class TestValueBoundaryScanner {

  @Test
  public void testLongEquals() {
    PTFExpressionDef argDef = new PTFExpressionDef();
    argDef.setOI(PrimitiveObjectInspectorFactory.writableLongObjectInspector);

    LongValueBoundaryScanner scanner =
        new LongValueBoundaryScanner(null, null, new OrderExpressionDef(argDef), false);
    LongWritable w1 = new LongWritable(1);
    LongWritable w2 = new LongWritable(2);

    Assert.assertTrue(scanner.isEqual(w1, w1));

    Assert.assertFalse(scanner.isEqual(w1, w2));
    Assert.assertFalse(scanner.isEqual(w2, w1));

    Assert.assertFalse(scanner.isEqual(null, w2));
    Assert.assertFalse(scanner.isEqual(w1, null));

    Assert.assertTrue(scanner.isEqual(null, null));
  }

  @Test
  public void testHiveDecimalEquals() {
    PTFExpressionDef argDef = new PTFExpressionDef();
    argDef.setOI(PrimitiveObjectInspectorFactory.writableHiveDecimalObjectInspector);

    HiveDecimalValueBoundaryScanner scanner =
        new HiveDecimalValueBoundaryScanner(null, null, new OrderExpressionDef(argDef), false);
    HiveDecimalWritable w1 = new HiveDecimalWritable(1);
    HiveDecimalWritable w2 = new HiveDecimalWritable(2);

    Assert.assertTrue(scanner.isEqual(w1, w1));

    Assert.assertFalse(scanner.isEqual(w1, w2));
    Assert.assertFalse(scanner.isEqual(w2, w1));

    Assert.assertFalse(scanner.isEqual(null, w2));
    Assert.assertFalse(scanner.isEqual(w1, null));

    Assert.assertTrue(scanner.isEqual(null, null));
  }

  @Test
  public void testDateEquals() {
    PTFExpressionDef argDef = new PTFExpressionDef();
    argDef.setOI(PrimitiveObjectInspectorFactory.writableDateObjectInspector);

    DateValueBoundaryScanner scanner =
        new DateValueBoundaryScanner(null, null, new OrderExpressionDef(argDef), false);
    Date date = new Date();
    date.setTimeInMillis(86400000); //epoch+1 day
    DateWritableV2 w1 = new DateWritableV2(date);
    DateWritableV2 w2 = new DateWritableV2(date);
    DateWritableV2 w3 = new DateWritableV2(); // empty

    Assert.assertTrue(scanner.isEqual(w1, w2));
    Assert.assertTrue(scanner.isEqual(w2, w1));

    // empty == epoch
    Assert.assertTrue(scanner.isEqual(w3, new DateWritableV2(new Date())));
    // empty != another non-epoch
    Assert.assertFalse(scanner.isEqual(w3, w1));

    Assert.assertFalse(scanner.isEqual(null, w2));
    Assert.assertFalse(scanner.isEqual(w1, null));

    Assert.assertTrue(scanner.isEqual(null, null));
  }

  @Test
  public void testTimestampEquals() {
    PTFExpressionDef argDef = new PTFExpressionDef();
    argDef.setOI(PrimitiveObjectInspectorFactory.writableTimestampObjectInspector);

    TimestampValueBoundaryScanner scanner =
        new TimestampValueBoundaryScanner(null, null, new OrderExpressionDef(argDef), false);
    Timestamp ts = new Timestamp();
    ts.setTimeInMillis(1000);

    TimestampWritableV2 w1 = new TimestampWritableV2(ts);
    TimestampWritableV2 w2 = new TimestampWritableV2(ts);
    TimestampWritableV2 w3 = new TimestampWritableV2(); // empty

    Assert.assertTrue(scanner.isEqual(w1, w2));
    Assert.assertTrue(scanner.isEqual(w2, w1));

    // empty == epoch
    Assert.assertTrue(scanner.isEqual(w3, new TimestampWritableV2(new Timestamp())));
    // empty != another non-epoch
    Assert.assertFalse(scanner.isEqual(w3, w1));

    Assert.assertFalse(scanner.isEqual(null, w2));
    Assert.assertFalse(scanner.isEqual(w1, null));

    Assert.assertTrue(scanner.isEqual(null, null));
  }

  @Test
  public void testTimestampLocalTZEquals() {
    PTFExpressionDef argDef = new PTFExpressionDef();
    argDef.setOI(PrimitiveObjectInspectorFactory.writableTimestampTZObjectInspector);

    TimestampLocalTZValueBoundaryScanner scanner =
        new TimestampLocalTZValueBoundaryScanner(null, null, new OrderExpressionDef(argDef), false);
    TimestampTZ ts = new TimestampTZ();
    ts.set(10, 0, ZoneId.systemDefault());

    TimestampLocalTZWritable w1 = new TimestampLocalTZWritable(ts);
    TimestampLocalTZWritable w2 = new TimestampLocalTZWritable(ts);
    TimestampLocalTZWritable w3 = new TimestampLocalTZWritable(); // empty
    w3.setTimeZone(ZoneId.of("UTC"));

    Assert.assertTrue(scanner.isEqual(w1, w2));
    Assert.assertTrue(scanner.isEqual(w2, w1));

    // empty == epoch
    TimestampTZ epoch = new TimestampTZ();
    epoch.set(0, 0, ZoneId.of("UTC"));
    Assert.assertTrue(scanner.isEqual(w3, new TimestampLocalTZWritable(epoch)));
    // empty != another non-epoch
    Assert.assertFalse(scanner.isEqual(w1, w3));

    Assert.assertFalse(scanner.isEqual(null, w2));
    Assert.assertFalse(scanner.isEqual(w1, null));

    Assert.assertTrue(scanner.isEqual(null, null));
  }

  @Test
  public void testStringEquals() {
    PTFExpressionDef argDef = new PTFExpressionDef();
    argDef.setOI(PrimitiveObjectInspectorFactory.writableStringObjectInspector);

    StringValueBoundaryScanner scanner =
        new StringValueBoundaryScanner(null, null, new OrderExpressionDef(argDef), false);
    Text w1 = new Text("a");
    Text w2 = new Text("b");

    Assert.assertTrue(scanner.isEqual(w1, w1));

    Assert.assertFalse(scanner.isEqual(w1, w2));
    Assert.assertFalse(scanner.isEqual(w2, w1));

    Assert.assertFalse(scanner.isEqual(null, w2));
    Assert.assertFalse(scanner.isEqual(w1, null));

    Assert.assertTrue(scanner.isEqual(null, null));
  }
}
