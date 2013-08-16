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

package org.apache.hadoop.hive.ql.exec;

import java.lang.reflect.Method;
import java.util.LinkedList;
import java.util.List;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;

public class TestFunctionRegistry extends TestCase {

  public class TestUDF {
    public void same(DoubleWritable x, DoubleWritable y) {}
    public void same(HiveDecimalWritable x, HiveDecimalWritable y) {}
    public void one(IntWritable x, HiveDecimalWritable y) {}
    public void one(IntWritable x, DoubleWritable y) {}
    public void one(IntWritable x, IntWritable y) {}
    public void mismatch(DateWritable x, HiveDecimalWritable y) {}
    public void mismatch(TimestampWritable x, HiveDecimalWritable y) {}
    public void mismatch(BytesWritable x, DoubleWritable y) {}
  }

  @Override
  protected void setUp() {
  }

  private void implicit(TypeInfo a, TypeInfo b, boolean convertible) {
    assertEquals(convertible, FunctionRegistry.implicitConvertable(a,b));
  }

  public void testImplicitConversion() {
    implicit(TypeInfoFactory.intTypeInfo, TypeInfoFactory.decimalTypeInfo, true);
    implicit(TypeInfoFactory.floatTypeInfo, TypeInfoFactory.decimalTypeInfo, true);
    implicit(TypeInfoFactory.doubleTypeInfo, TypeInfoFactory.decimalTypeInfo, true);
    implicit(TypeInfoFactory.stringTypeInfo, TypeInfoFactory.decimalTypeInfo, true);
    implicit(TypeInfoFactory.dateTypeInfo, TypeInfoFactory.decimalTypeInfo, false);
    implicit(TypeInfoFactory.timestampTypeInfo, TypeInfoFactory.decimalTypeInfo, false);
  }

  private void verify(Class udf, String name, TypeInfo ta, TypeInfo tb,
                      Class a, Class b, boolean throwException) {
    List<TypeInfo> args = new LinkedList<TypeInfo>();
    args.add(ta);
    args.add(tb);

    Method result = null;

    try {
      result = FunctionRegistry.getMethodInternal(udf, name, false, args);
    } catch (UDFArgumentException e) {
      assert(throwException);
      return;
    }
    assert(!throwException);
    assertEquals(2, result.getParameterTypes().length);
    assertEquals(result.getParameterTypes()[0], a);
    assertEquals(result.getParameterTypes()[1], b);
  }

  public void testGetMethodInternal() {

    verify(TestUDF.class, "same", TypeInfoFactory.intTypeInfo, TypeInfoFactory.intTypeInfo,
           DoubleWritable.class, DoubleWritable.class, false);

    verify(TestUDF.class, "same", TypeInfoFactory.doubleTypeInfo, TypeInfoFactory.doubleTypeInfo,
           DoubleWritable.class, DoubleWritable.class, false);

    verify(TestUDF.class, "same", TypeInfoFactory.doubleTypeInfo, TypeInfoFactory.decimalTypeInfo,
           HiveDecimalWritable.class, HiveDecimalWritable.class, false);

    verify(TestUDF.class, "same", TypeInfoFactory.decimalTypeInfo, TypeInfoFactory.doubleTypeInfo,
           HiveDecimalWritable.class, HiveDecimalWritable.class, false);

    verify(TestUDF.class, "same", TypeInfoFactory.decimalTypeInfo, TypeInfoFactory.decimalTypeInfo,
           HiveDecimalWritable.class, HiveDecimalWritable.class, false);

    verify(TestUDF.class, "one", TypeInfoFactory.intTypeInfo, TypeInfoFactory.decimalTypeInfo,
           IntWritable.class, HiveDecimalWritable.class, false);

    verify(TestUDF.class, "one", TypeInfoFactory.intTypeInfo, TypeInfoFactory.floatTypeInfo,
           IntWritable.class, DoubleWritable.class, false);

    verify(TestUDF.class, "one", TypeInfoFactory.intTypeInfo, TypeInfoFactory.intTypeInfo,
           IntWritable.class, IntWritable.class, false);

    verify(TestUDF.class, "mismatch", TypeInfoFactory.voidTypeInfo, TypeInfoFactory.intTypeInfo,
           null, null, true);
  }

  private void common(TypeInfo a, TypeInfo b, TypeInfo result) {
    assertEquals(FunctionRegistry.getCommonClass(a,b), result);
  }

  public void testCommonClass() {
    common(TypeInfoFactory.intTypeInfo, TypeInfoFactory.decimalTypeInfo,
           TypeInfoFactory.decimalTypeInfo);
    common(TypeInfoFactory.stringTypeInfo, TypeInfoFactory.decimalTypeInfo,
           TypeInfoFactory.stringTypeInfo);
    common(TypeInfoFactory.doubleTypeInfo, TypeInfoFactory.decimalTypeInfo,
           TypeInfoFactory.decimalTypeInfo);
    common(TypeInfoFactory.doubleTypeInfo, TypeInfoFactory.stringTypeInfo,
           TypeInfoFactory.stringTypeInfo);
  }

  private void comparison(TypeInfo a, TypeInfo b, TypeInfo result) {
    assertEquals(FunctionRegistry.getCommonClassForComparison(a,b), result);
  }

  public void testCommonClassComparison() {
    comparison(TypeInfoFactory.intTypeInfo, TypeInfoFactory.decimalTypeInfo,
               TypeInfoFactory.decimalTypeInfo);
    comparison(TypeInfoFactory.stringTypeInfo, TypeInfoFactory.decimalTypeInfo,
               TypeInfoFactory.decimalTypeInfo);
    comparison(TypeInfoFactory.doubleTypeInfo, TypeInfoFactory.decimalTypeInfo,
               TypeInfoFactory.decimalTypeInfo);
    comparison(TypeInfoFactory.doubleTypeInfo, TypeInfoFactory.stringTypeInfo,
               TypeInfoFactory.doubleTypeInfo);
  }

  @Override
  protected void tearDown() {
  }

  public void testIsRankingFunction() {
    Assert.assertTrue(FunctionRegistry.isRankingFunction("rank"));
    Assert.assertTrue(FunctionRegistry.isRankingFunction("dense_rank"));
    Assert.assertTrue(FunctionRegistry.isRankingFunction("percent_rank"));
    Assert.assertTrue(FunctionRegistry.isRankingFunction("cume_dist"));
    Assert.assertFalse(FunctionRegistry.isRankingFunction("min"));
  }

  public void testImpliesOrder() {
    Assert.assertTrue(FunctionRegistry.impliesOrder("rank"));
    Assert.assertTrue(FunctionRegistry.impliesOrder("dense_rank"));
    Assert.assertTrue(FunctionRegistry.impliesOrder("percent_rank"));
    Assert.assertTrue(FunctionRegistry.impliesOrder("cume_dist"));
    Assert.assertTrue(FunctionRegistry.impliesOrder("first_value"));
    Assert.assertTrue(FunctionRegistry.impliesOrder("last_value"));
    Assert.assertTrue(FunctionRegistry.impliesOrder("lead"));
    Assert.assertTrue(FunctionRegistry.impliesOrder("lag"));
    Assert.assertFalse(FunctionRegistry.impliesOrder("min"));
  }
}
