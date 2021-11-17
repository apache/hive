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

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category.PRIMITIVE;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category.UNION;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardListObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardMapObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardStructObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardUnionObjectInspector;

/**
 * Tests for {@link GenericUDF#initialize(ObjectInspector[])} on the most common equality/comparison operators when 
 * arguments of various types are provided.
 * 
 * The tests cover positive and negative cases for the following operators:
 * <ul>
 * <li>{@link GenericUDFOPEqual}</li>
 * <li>{@link GenericUDFOPEqualNS}</li>
 * <li>{@link GenericUDFOPNotEqual}</li>
 * <li>{@link GenericUDFOPNotEqualNS}</li>
 * <li>{@link GenericUDFOPGreaterThan}</li>
 * <li>{@link GenericUDFOPEqualOrGreaterThan}</li>
 * <li>{@link GenericUDFOPLessThan}</li>
 * <li>{@link GenericUDFOPEqualOrLessThan}</li>
 * <li>{@link GenericUDFIn}</li>
 * </ul>
 *
 */
public class TestGenericUDFInitializeOnCompareUDF {

  private static Collection<UDFArguments> generateArguments() {
    List<UDFArguments> arguments = new ArrayList<>();

    List<ObjectInspector> primitives = new ArrayList<>();
    for (PrimitiveObjectInspector.PrimitiveCategory l : PrimitiveObjectInspector.PrimitiveCategory.values()) {
      if (l == PrimitiveObjectInspector.PrimitiveCategory.UNKNOWN) {
        continue;
      }
      primitives.add(PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(l));
    }

    List<ObjectInspector> nonPrimitives = new ArrayList<>();
    for (ObjectInspector i : primitives) {
      nonPrimitives.add(getStandardListObjectInspector(i));
      nonPrimitives.add(getStandardStructObjectInspector(Arrays.asList("field1", "field2"), Arrays.asList(i, i)));
      nonPrimitives.add(getStandardUnionObjectInspector(Arrays.asList(i, i)));
      nonPrimitives.add(getStandardMapObjectInspector(i, i));
    }

    List<ObjectInspector> allTypes = new ArrayList<>(primitives);
    allTypes.addAll(nonPrimitives);
    for (ObjectInspector firstArgInspector : allTypes) {
      for (ObjectInspector secondArgInspector : allTypes) {
        arguments.add(new UDFArguments(firstArgInspector, secondArgInspector));
      }
    }
    return arguments;
  }

  private static Stream<UDFArguments> generateArgsWithDifferentCategories() {
    return generateArguments().stream().filter(args -> !args.left.getCategory().equals(args.right.getCategory()));
  }

  private static Stream<UDFArguments> generateArgsWithSameCategoryNoBothPrimitive() {
    return generateArguments().stream().filter(args -> {
      ObjectInspector.Category left = args.left.getCategory();
      ObjectInspector.Category right = args.right.getCategory();
      return left.equals(right) && !(PRIMITIVE.equals(left) && PRIMITIVE.equals(right));
    });
  }

  private static Stream<UDFArguments> generateArgsWithSameTypesNoCategoryUnion() {
    return generateArguments().stream().filter(args -> {
      TypeInfo left = TypeInfoUtils.getTypeInfoFromObjectInspector(args.left);
      TypeInfo right = TypeInfoUtils.getTypeInfoFromObjectInspector(args.right);
      return left.equals(right) && !UNION.equals(left.getCategory()) && !UNION.equals(right.getCategory());
    });
  }

  @ParameterizedTest
  @MethodSource("generateArgsWithDifferentCategories")
  public void testArgsWithDifferentTypeCategoriesThrowsException(UDFArguments args) {
    List<GenericUDF> udfs = Arrays.asList(
        new GenericUDFOPEqual(),
        new GenericUDFOPEqualNS(),
        new GenericUDFOPNotEqual(),
        new GenericUDFOPEqualNS(),
        new GenericUDFIn(),
        new GenericUDFOPEqualOrLessThan(),
        new GenericUDFOPEqualOrGreaterThan(),
        new GenericUDFOPLessThan(),
        new GenericUDFOPGreaterThan());
    for (GenericUDF u : udfs) {
      try {
        u.initialize(new ObjectInspector[] { args.left, args.right });
      }catch (UDFArgumentException e){
        Assertions.assertTrue(e.getMessage().contains("Type mismatch"), "Unexpected message for " + u.getUdfName());
      }
    }
  }
  
  @ParameterizedTest
  @MethodSource("generateArgsWithSameTypesNoCategoryUnion")
  public void testEqualityUDFWithSameTypeArgsSucceeds(UDFArguments args) throws UDFArgumentException {
    List<GenericUDF> udfs = Arrays.asList(
        new GenericUDFOPEqual(),
        new GenericUDFOPEqualNS(),
        new GenericUDFOPNotEqual(),
        new GenericUDFOPEqualNS(),
        new GenericUDFIn());
    for (GenericUDF u : udfs) {
      u.initialize(new ObjectInspector[] { args.left, args.right });
    }
  }

  @ParameterizedTest
  @MethodSource("generateArgsWithSameCategoryNoBothPrimitive")
  public void testBaseNonEqualityUDFWithNonPrimitiveTypeArgsThrowsException(UDFArguments args) {
    List<GenericUDF> udfs = Arrays
        .asList(new GenericUDFOPGreaterThan(), new GenericUDFOPLessThan(), new GenericUDFOPEqualOrGreaterThan(),
            new GenericUDFOPEqualOrLessThan());
    for (GenericUDF udf : udfs) {
      try {
        udf.initialize(new ObjectInspector[] { args.left, args.right });
        Assertions.fail(
            udf.getUdfName() + " operator should not accept non primitive types [" + args.left.getCategory() + ","
                + args.right.getCategory() + "]");
      } catch (UDFArgumentException e) {
        boolean isValidMessage =
            e.getMessage().contains("not support MAP types") ||
            e.getMessage().contains("not support LIST types") ||
            e.getMessage().contains("not support STRUCT types") ||
            e.getMessage().contains("not support UNION types");
        Assertions.assertTrue(isValidMessage, "Unexpected message for " + udf.getUdfName());
      }
    }
  }

  private static class UDFArguments {
    final ObjectInspector left;
    final ObjectInspector right;

    UDFArguments(ObjectInspector left, ObjectInspector right) {
      this.left = left;
      this.right = right;
    }

    @Override
    public String toString() {
      TypeInfo til = TypeInfoUtils.getTypeInfoFromObjectInspector(left);
      TypeInfo tir = TypeInfoUtils.getTypeInfoFromObjectInspector(right);
      return "(" + til + ", " + tir + ")";
    }
  }
}
