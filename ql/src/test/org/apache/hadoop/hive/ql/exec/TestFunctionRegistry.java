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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class TestFunctionRegistry extends TestCase {

  public class TestUDF {
    public void same(DoubleWritable x, DoubleWritable y) {}
    public void same(HiveDecimalWritable x, HiveDecimalWritable y) {}
    public void same(Text x, Text y) {}
    public void one(IntWritable x, HiveDecimalWritable y) {}
    public void one(IntWritable x, DoubleWritable y) {}
    public void one(IntWritable x, IntWritable y) {}
    public void mismatch(DateWritable x, HiveDecimalWritable y) {}
    public void mismatch(TimestampWritable x, HiveDecimalWritable y) {}
    public void mismatch(BytesWritable x, DoubleWritable y) {}
    public void typeaffinity1(DateWritable x) {}
    public void typeaffinity1(DoubleWritable x) {};
    public void typeaffinity1(Text x) {}
    public void typeaffinity2(IntWritable x) {}
    public void typeaffinity2(DoubleWritable x) {}
  }

  TypeInfo varchar5;
  TypeInfo varchar10;
  TypeInfo maxVarchar;
  TypeInfo char5;
  TypeInfo char10;

  @Override
  protected void setUp() {
    String maxVarcharTypeName = "varchar(" + HiveVarchar.MAX_VARCHAR_LENGTH + ")";
    maxVarchar = TypeInfoFactory.getPrimitiveTypeInfo(maxVarcharTypeName);
    varchar10 = TypeInfoFactory.getPrimitiveTypeInfo("varchar(10)");
    varchar5 = TypeInfoFactory.getPrimitiveTypeInfo("varchar(5)");
    char10 = TypeInfoFactory.getPrimitiveTypeInfo("char(10)");
    char5 = TypeInfoFactory.getPrimitiveTypeInfo("char(5)");
    SessionState.start(new HiveConf());
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
    implicit(varchar10, TypeInfoFactory.stringTypeInfo, true);
    implicit(TypeInfoFactory.stringTypeInfo, varchar10, true);

    // Try with parameterized varchar types
    TypeInfo varchar10 = TypeInfoFactory.getPrimitiveTypeInfo("varchar(10)");
    TypeInfo varchar20 = TypeInfoFactory.getPrimitiveTypeInfo("varchar(20)");

    implicit(varchar10, TypeInfoFactory.stringTypeInfo, true);
    implicit(varchar20, TypeInfoFactory.stringTypeInfo, true);
    implicit(TypeInfoFactory.stringTypeInfo, varchar10, true);
    implicit(TypeInfoFactory.stringTypeInfo, varchar20, true);
    implicit(varchar20, varchar10, true);

    implicit(char10, TypeInfoFactory.stringTypeInfo, true);
    implicit(TypeInfoFactory.stringTypeInfo, char10, true);
    implicit(char5, char10, true);
    implicit(char5, varchar10, true);
    implicit(varchar5, char10, true);

    implicit(TypeInfoFactory.intTypeInfo, char10, true);
    implicit(TypeInfoFactory.intTypeInfo, varchar10, true);
    implicit(TypeInfoFactory.intTypeInfo, TypeInfoFactory.stringTypeInfo, true);
  }

  private static List<Method> getMethods(Class<?> udfClass, String methodName) {
    List<Method> mlist = new ArrayList<Method>();

    for (Method m : udfClass.getMethods()) {
      if (m.getName().equals(methodName)) {
        mlist.add(m);
      }
    }
    return mlist;
  }

  private void typeAffinity(String methodName, TypeInfo inputType,
      int expectedNumFoundMethods, Class expectedFoundType) {
    List<Method> mlist = getMethods(TestUDF.class, methodName);
    assertEquals(true, 1 < mlist.size());
    List<TypeInfo> inputTypes = new ArrayList<TypeInfo>();
    inputTypes.add(inputType);

    // narrow down the possible choices based on type affinity
    FunctionRegistry.filterMethodsByTypeAffinity(mlist, inputTypes);
    assertEquals(expectedNumFoundMethods, mlist.size());
    if (expectedNumFoundMethods == 1) {
      assertEquals(expectedFoundType, mlist.get(0).getParameterTypes()[0]);
    }
  }

  public void testTypeAffinity() {
    // Prefer numeric type arguments over other method signatures
    typeAffinity("typeaffinity1", TypeInfoFactory.shortTypeInfo, 1, DoubleWritable.class);
    typeAffinity("typeaffinity1", TypeInfoFactory.intTypeInfo, 1, DoubleWritable.class);
    typeAffinity("typeaffinity1", TypeInfoFactory.floatTypeInfo, 1, DoubleWritable.class);

    // Prefer date type arguments over other method signatures
    typeAffinity("typeaffinity1", TypeInfoFactory.dateTypeInfo, 1, DateWritable.class);
    typeAffinity("typeaffinity1", TypeInfoFactory.timestampTypeInfo, 1, DateWritable.class);

    // String type affinity
    typeAffinity("typeaffinity1", TypeInfoFactory.stringTypeInfo, 1, Text.class);
    typeAffinity("typeaffinity1", char5, 1, Text.class);
    typeAffinity("typeaffinity1", varchar5, 1, Text.class);

    // Type affinity does not help when multiple methods have the same type affinity.
    typeAffinity("typeaffinity2", TypeInfoFactory.shortTypeInfo, 2, null);

    // Type affinity does not help when type affinity does not match input args
    typeAffinity("typeaffinity2", TypeInfoFactory.dateTypeInfo, 2, null);
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
    assertEquals(a, result.getParameterTypes()[0]);
    assertEquals(b, result.getParameterTypes()[1]);
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

    // Passing char/varchar arguments should prefer the version of evaluate() with Text args.
    verify(TestUDF.class, "same", varchar5, varchar10, Text.class, Text.class, false);
    verify(TestUDF.class, "same", char5, char10, Text.class, Text.class, false);

    verify(TestUDF.class, "mismatch", TypeInfoFactory.voidTypeInfo, TypeInfoFactory.intTypeInfo,
           null, null, true);
  }

  private void common(TypeInfo a, TypeInfo b, TypeInfo result) {
    assertEquals(result, FunctionRegistry.getCommonClass(a,b));
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

    common(TypeInfoFactory.stringTypeInfo, varchar10, TypeInfoFactory.stringTypeInfo);
    common(varchar10, TypeInfoFactory.stringTypeInfo, TypeInfoFactory.stringTypeInfo);
    common(TypeInfoFactory.stringTypeInfo, char10, TypeInfoFactory.stringTypeInfo);
    common(char10, TypeInfoFactory.stringTypeInfo, TypeInfoFactory.stringTypeInfo);
    // common class between char/varchar is string?
    common(char5, varchar10, TypeInfoFactory.stringTypeInfo);
  }

  private void comparison(TypeInfo a, TypeInfo b, TypeInfo result) {
    assertEquals(result, FunctionRegistry.getCommonClassForComparison(a,b));
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

    comparison(TypeInfoFactory.dateTypeInfo, TypeInfoFactory.stringTypeInfo,
        TypeInfoFactory.stringTypeInfo);
    comparison(TypeInfoFactory.stringTypeInfo, TypeInfoFactory.dateTypeInfo,
        TypeInfoFactory.stringTypeInfo);

    comparison(TypeInfoFactory.stringTypeInfo, varchar10, TypeInfoFactory.stringTypeInfo);
    comparison(varchar10, TypeInfoFactory.stringTypeInfo, TypeInfoFactory.stringTypeInfo);
    comparison(varchar5, varchar10, varchar10);
    comparison(TypeInfoFactory.stringTypeInfo, char10, TypeInfoFactory.stringTypeInfo);
    comparison(char10, TypeInfoFactory.stringTypeInfo, TypeInfoFactory.stringTypeInfo);
    comparison(char5, char10, char10);
    // common comparison class for char/varchar is string?
    comparison(char10, varchar5, TypeInfoFactory.stringTypeInfo);
  }

  /**
   * Method to print out the comparison/conversion behavior for data types.
   */
  public void testPrintTypeCompatibility() {
    if (true) {
      return;
    }

    String[] typeStrings = {
        "void", "boolean", "tinyint", "smallint", "int", "bigint", "float", "double",
        "string", "timestamp", "date", "binary", "decimal", "varchar(10)", "varchar(5)",
    };
    for (String cat1 : typeStrings) {
      TypeInfo ti1 = null;
      try {
        ti1 = TypeInfoUtils.getTypeInfoFromTypeString(cat1);
      } catch (Exception err) {
        System.out.println(err);
        System.out.println("Unable to get TypeInfo for " + cat1 + ", skipping ...");
        continue;
      }

      for (String cat2 : typeStrings) {
        TypeInfo commonClass = null;
        boolean implicitConvertable = false;
        try {
          TypeInfo ti2 = TypeInfoUtils.getTypeInfoFromTypeString(cat2);
          try {
            commonClass = FunctionRegistry.getCommonClassForComparison(ti1, ti2);
            //implicitConvertable = FunctionRegistry.implicitConvertable(ti1, ti2);
          } catch (Exception err) {
            System.out.println("Failed to get common class for " + ti1 + ", " + ti2 + ": " + err);
            err.printStackTrace();
            //System.out.println("Unable to get TypeInfo for " + cat2 + ", skipping ...");
          }
          System.out.println(cat1 + " - " + cat2 + ": " + commonClass);
          //System.out.println(cat1 + " - " + cat2 + ": " + implicitConvertable);
        } catch (Exception err) {
          System.out.println(err);
          System.out.println("Unable to get TypeInfo for " + cat2 + ", skipping ...");
          continue;
        }
      }
    }
  }

  private void unionAll(TypeInfo a, TypeInfo b, TypeInfo result) {
    assertEquals(result, FunctionRegistry.getCommonClassForUnionAll(a,b));
  }

  public void testCommonClassUnionAll() {
    unionAll(TypeInfoFactory.intTypeInfo, TypeInfoFactory.decimalTypeInfo,
        TypeInfoFactory.decimalTypeInfo);
    unionAll(TypeInfoFactory.stringTypeInfo, TypeInfoFactory.decimalTypeInfo,
        TypeInfoFactory.decimalTypeInfo);
    unionAll(TypeInfoFactory.doubleTypeInfo, TypeInfoFactory.decimalTypeInfo,
        TypeInfoFactory.decimalTypeInfo);
    unionAll(TypeInfoFactory.doubleTypeInfo, TypeInfoFactory.stringTypeInfo,
        TypeInfoFactory.stringTypeInfo);

    unionAll(varchar5, varchar10, varchar10);
    unionAll(varchar10, varchar5, varchar10);
    unionAll(varchar10, TypeInfoFactory.stringTypeInfo, TypeInfoFactory.stringTypeInfo);
    unionAll(TypeInfoFactory.stringTypeInfo, varchar10, TypeInfoFactory.stringTypeInfo);

    unionAll(char5, char10, char10);
    unionAll(char10, char5, char10);
    unionAll(char10, TypeInfoFactory.stringTypeInfo, TypeInfoFactory.stringTypeInfo);
    unionAll(TypeInfoFactory.stringTypeInfo, char10, TypeInfoFactory.stringTypeInfo);

    // common class for char/varchar is string?
    comparison(char10, varchar5, TypeInfoFactory.stringTypeInfo);
  }

  public void testGetTypeInfoForPrimitiveCategory() {
    // varchar should take string length into account.
    // varchar(5), varchar(10) => varchar(10)
    assertEquals(varchar10, FunctionRegistry.getTypeInfoForPrimitiveCategory(
        (PrimitiveTypeInfo) varchar5, (PrimitiveTypeInfo) varchar10, PrimitiveCategory.VARCHAR));
    assertEquals(varchar10, FunctionRegistry.getTypeInfoForPrimitiveCategory(
        (PrimitiveTypeInfo) varchar10, (PrimitiveTypeInfo) varchar5, PrimitiveCategory.VARCHAR));

    assertEquals(char10, FunctionRegistry.getTypeInfoForPrimitiveCategory(
        (PrimitiveTypeInfo) char5, (PrimitiveTypeInfo) char10, PrimitiveCategory.CHAR));
    assertEquals(char10, FunctionRegistry.getTypeInfoForPrimitiveCategory(
        (PrimitiveTypeInfo) char10, (PrimitiveTypeInfo) char5, PrimitiveCategory.CHAR));

    assertEquals(varchar10, FunctionRegistry.getTypeInfoForPrimitiveCategory(
        (PrimitiveTypeInfo) varchar5, (PrimitiveTypeInfo) char10, PrimitiveCategory.VARCHAR));

    // non-qualified types should simply return the TypeInfo associated with that type
    assertEquals(TypeInfoFactory.stringTypeInfo, FunctionRegistry.getTypeInfoForPrimitiveCategory(
        (PrimitiveTypeInfo) varchar10, (PrimitiveTypeInfo) TypeInfoFactory.stringTypeInfo,
        PrimitiveCategory.STRING));
    assertEquals(TypeInfoFactory.stringTypeInfo, FunctionRegistry.getTypeInfoForPrimitiveCategory(
        (PrimitiveTypeInfo) TypeInfoFactory.stringTypeInfo,
        (PrimitiveTypeInfo) TypeInfoFactory.stringTypeInfo,
        PrimitiveCategory.STRING));
    assertEquals(TypeInfoFactory.doubleTypeInfo, FunctionRegistry.getTypeInfoForPrimitiveCategory(
        (PrimitiveTypeInfo) TypeInfoFactory.doubleTypeInfo,
        (PrimitiveTypeInfo) TypeInfoFactory.stringTypeInfo,
        PrimitiveCategory.DOUBLE));
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
