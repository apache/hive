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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hive.ql.udf.generic;

import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.udf.esri.ST_GeomFromGeoJson;
import org.apache.hadoop.hive.ql.udf.esri.ST_GeomFromJson;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.Mode;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructEncodedVariantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.variant.Variant;
import org.apache.hadoop.hive.serde2.variant.VariantBuilder;
import org.apache.hadoop.hive.serde2.variant.VariantEncoding;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestGenericUDFsOnVariant {
  private static final ObjectInspector STANDARD = ObjectInspectorFactory.getStandardVariantObjectInspector();
  private static final ObjectInspector ENCODED =
      new StructEncodedVariantObjectInspector(VariantEncoding.JAVA_OBJECT_INSPECTOR);
  private static final ObjectInspector STRING = PrimitiveObjectInspectorFactory.javaStringObjectInspector;

  private static Variant variant(String json) throws Exception {
    return VariantBuilder.parseJson(json, false);
  }

  private static Object eval(GenericUDF udf, Object... args) throws Exception {
    GenericUDF.DeferredObject[] deferred = new GenericUDF.DeferredObject[args.length];
    for (int i = 0; i < args.length; i++) {
      deferred[i] = new GenericUDF.DeferredJavaObject(args[i]);
    }
    return udf.evaluate(deferred);
  }

  private static boolean evalBoolean(GenericUDF udf, Object... args) throws Exception {
    return ((BooleanWritable) eval(udf, args)).get();
  }

  @Test
  void parseJsonReturnsAVariant() throws Exception {
    GenericUDFParseJson udf = new GenericUDFParseJson();
    assertSame(STANDARD, udf.initialize(new ObjectInspector[] {STRING}));
    Variant result = assertInstanceOf(Variant.class, eval(udf, "{\"a\":5}"));
    assertEquals("{\"a\":5}", result.toJson(ZoneOffset.UTC));
  }

  @Test
  void variantGetAndToJsonReadEveryVariantInspector() throws Exception {
    Variant v = variant("{\"a\":7}");
    ObjectInspector intType = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.stringTypeInfo, new Text("int"));
    for (ObjectInspector oi : List.of(STANDARD, ENCODED)) {
      Object data = oi == STANDARD ? v : VariantEncoding.fields(v);

      GenericUDFVariantGet get = new GenericUDFVariantGet();
      get.initialize(new ObjectInspector[] {oi, STRING, intType});
      assertEquals(7, eval(get, data, "$.a"));

      GenericUDFToJson toJson = new GenericUDFToJson();
      toJson.initialize(new ObjectInspector[] {oi});
      assertEquals("{\"a\":7}", eval(toJson, data));
    }
  }

  @Test
  void variantFunctionsRejectAStruct() {
    ObjectInspector struct = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(VariantEncoding.TYPE_INFO);
    assertThrows(UDFArgumentException.class,
        () -> new GenericUDFVariantGet().initialize(new ObjectInspector[] {struct, STRING}));
    assertThrows(UDFArgumentException.class,
        () -> new GenericUDFTryVariantGet().initialize(new ObjectInspector[] {struct, STRING}));
    assertThrows(UDFArgumentException.class, () -> new GenericUDFToJson().initialize(new ObjectInspector[] {struct}));
  }

  private static List<?> collect(AbstractGenericUDAFResolver resolver, String... jsons) throws Exception {
    GenericUDAFEvaluator evaluator = resolver.getEvaluator(new TypeInfo[] {TypeInfoFactory.getVariantTypeInfo()});
    evaluator.init(Mode.COMPLETE, new ObjectInspector[] {ENCODED});
    AggregationBuffer buffer = evaluator.getNewAggregationBuffer();
    for (String json : jsons) {
      evaluator.iterate(buffer, new Object[] {VariantEncoding.fields(variant(json))});
    }
    return (List<?>) evaluator.terminate(buffer);
  }

  @Test
  void collectListKeepsEveryVariantAndCollectSetDedupesEqualBytes() throws Exception {
    assertEquals(List.of(variant("{\"k\":1}"), variant("{\"k\":1}"), variant("{\"k\":2}")),
        collect(new GenericUDAFCollectList(), "{\"k\":1}", "{\"k\":1}", "{\"k\":2}"));
    assertEquals(List.of(variant("{\"k\":1}"), variant("{\"k\":2}")),
        collect(new GenericUDAFCollectSet(), "{\"k\":1}", "{\"k\":1}", "{\"k\":2}"));
  }

  @Test
  void approxDistinctCountsEqualVariantsOnce() throws Exception {
    GenericUDAFEvaluator evaluator = new GenericUDAFApproximateDistinct()
        .getEvaluator(new TypeInfo[] {TypeInfoFactory.getVariantTypeInfo()});
    // COMPLETE does not set the input inspector in this evaluator; PARTIAL1 is the map-side mode
    evaluator.init(Mode.PARTIAL1, new ObjectInspector[] {STANDARD});
    AggregationBuffer buffer = evaluator.getNewAggregationBuffer();
    for (String json : List.of("[1,2]", "[1,2]", "{\"a\":1}")) {
      evaluator.iterate(buffer, new Object[] {variant(json)});
    }
    assertEquals(2L, ((LongWritable) evaluator.terminate(buffer)).get());
  }

  private static byte[] approxDistinctSketch(ObjectInspector oi, Object... rows) throws Exception {
    GenericUDAFEvaluator evaluator = new GenericUDAFApproximateDistinct()
        .getEvaluator(new TypeInfo[] {TypeInfoFactory.getVariantTypeInfo()});
    evaluator.init(Mode.PARTIAL1, new ObjectInspector[] {oi});
    AggregationBuffer buffer = evaluator.getNewAggregationBuffer();
    for (Object row : rows) {
      evaluator.iterate(buffer, new Object[] {row});
    }
    BytesWritable partial = (BytesWritable) evaluator.terminatePartial(buffer);
    return Arrays.copyOf(partial.getBytes(), partial.getLength());
  }

  @Test
  void approxDistinctSketchIsTheEncodingStructSketch() throws Exception {
    Variant[] variants = {variant("[1,2]"), variant("{\"a\":1}"), variant("\"x\"")};
    Object[] encoded = new Object[variants.length];
    for (int i = 0; i < variants.length; i++) {
      encoded[i] = VariantEncoding.fields(variants[i]);
    }
    assertArrayEquals(approxDistinctSketch(VariantEncoding.JAVA_OBJECT_INSPECTOR, encoded),
        approxDistinctSketch(STANDARD, (Object[]) variants));
  }

  @Test
  void equalityOperatorsCompareTheBytes() throws Exception {
    GenericUDFOPEqual eq = new GenericUDFOPEqual();
    eq.initialize(new ObjectInspector[] {ENCODED, STANDARD});
    assertTrue(evalBoolean(eq, VariantEncoding.fields(variant("[1,2]")), variant("[1,2]")));
    assertFalse(evalBoolean(eq, VariantEncoding.fields(variant("[1,2]")), variant("[2,1]")));

    GenericUDFOPNotEqual ne = new GenericUDFOPNotEqual();
    ne.initialize(new ObjectInspector[] {STANDARD, STANDARD});
    assertTrue(evalBoolean(ne, variant("1"), variant("2")));

    GenericUDFOPEqualNS ns = new GenericUDFOPEqualNS();
    ns.initialize(new ObjectInspector[] {STANDARD, STANDARD});
    assertTrue(evalBoolean(ns, null, null));
    assertTrue(evalBoolean(ns, variant("\"x\""), variant("\"x\"")));
  }

  @Test
  void inMatchesConstantAndNonConstantVariants() throws Exception {
    ObjectInspector one = ObjectInspectorFactory.getStandardConstantVariantObjectInspector(variant("1"));
    ObjectInspector a = ObjectInspectorFactory.getStandardConstantVariantObjectInspector(variant("\"a\""));
    GenericUDFIn constants = new GenericUDFIn();
    constants.initialize(new ObjectInspector[] {STANDARD, one, a});
    assertTrue(evalBoolean(constants, variant("1"), null, null));
    assertFalse(evalBoolean(constants, variant("2"), null, null));

    // the struct-encoded left side goes through a converting VariantConverter
    GenericUDFIn encodedConstants = new GenericUDFIn();
    encodedConstants.initialize(new ObjectInspector[] {ENCODED, one, a});
    assertTrue(evalBoolean(encodedConstants, VariantEncoding.fields(variant("1")), null, null));
    assertFalse(evalBoolean(encodedConstants, VariantEncoding.fields(variant("2")), null, null));

    GenericUDFIn columns = new GenericUDFIn();
    columns.initialize(new ObjectInspector[] {ENCODED, STANDARD, STANDARD});
    assertTrue(evalBoolean(columns, VariantEncoding.fields(variant("1")), variant("2"), variant("1")));
  }

  @Test
  void inFollowsSqlNullSemantics() throws Exception {
    ObjectInspector one = ObjectInspectorFactory.getStandardConstantVariantObjectInspector(variant("1"));
    ObjectInspector nullConstant = ObjectInspectorFactory.getStandardConstantVariantObjectInspector(null);
    GenericUDFIn constants = new GenericUDFIn();
    constants.initialize(new ObjectInspector[] {STANDARD, one, nullConstant});
    assertNull(eval(constants, null, null, null));
    assertTrue(evalBoolean(constants, variant("1"), null, null));
    assertNull(eval(constants, variant("2"), null, null));

    GenericUDFIn columns = new GenericUDFIn();
    columns.initialize(new ObjectInspector[] {STANDARD, STANDARD, STANDARD});
    assertNull(eval(columns, null, variant("1"), variant("2")));
    assertTrue(evalBoolean(columns, variant("1"), variant("1"), null));
    assertNull(eval(columns, variant("2"), variant("1"), null));
    assertFalse(evalBoolean(columns, variant("2"), variant("1"), variant("3")));
  }

  @Test
  void orderingOperatorsRejectAVariant() {
    ObjectInspector[] variants = {STANDARD, STANDARD};
    assertEquals("LESS THAN does not support VARIANT types",
        assertThrows(UDFArgumentException.class, () -> new GenericUDFOPLessThan().initialize(variants)).getMessage());
    assertEquals("GREATER THAN does not support VARIANT types",
        assertThrows(UDFArgumentException.class, () -> new GenericUDFOPGreaterThan().initialize(variants))
            .getMessage());
  }

  @Test
  void nullifComparesVariants() throws Exception {
    GenericUDFNullif nullif = new GenericUDFNullif();
    nullif.initialize(new ObjectInspector[] {STANDARD, STANDARD});
    assertNull(eval(nullif, variant("{\"k\":1}"), variant("{\"k\":1}")));
    assertEquals(variant("{\"k\":1}"), eval(nullif, variant("{\"k\":1}"), variant("{\"k\":2}")));
  }

  @Test
  void esriJsonFunctionsAcceptOnlyAString() {
    assertThrows(UDFArgumentException.class,
        () -> new ST_GeomFromJson().initialize(new ObjectInspector[] {STANDARD}));
    assertThrows(UDFArgumentException.class,
        () -> new ST_GeomFromGeoJson().initialize(new ObjectInspector[] {STANDARD}));
    assertDoesNotThrow(() -> new ST_GeomFromJson().initialize(new ObjectInspector[] {STRING}));
    assertDoesNotThrow(() -> new ST_GeomFromGeoJson().initialize(new ObjectInspector[] {STRING}));
  }
}
