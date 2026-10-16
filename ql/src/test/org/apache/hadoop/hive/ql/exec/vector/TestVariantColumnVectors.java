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
package org.apache.hadoop.hive.ql.exec.vector;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.common.type.DataTypePhysicalVariation;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.vector.expressions.IsNull;
import org.apache.hadoop.hive.ql.exec.vector.expressions.SelectColumnIsNull;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpressionWriter;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpressionWriterFactory;
import org.apache.hadoop.hive.ql.exec.vector.udf.VectorUDFAdaptor;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNull;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToJson;
import org.apache.hadoop.hive.serde2.ByteStream.Output;
import org.apache.hadoop.hive.serde2.binarysortable.BinarySortableSerDe;
import org.apache.hadoop.hive.serde2.binarysortable.fast.BinarySortableDeserializeRead;
import org.apache.hadoop.hive.serde2.binarysortable.fast.BinarySortableSerializeWrite;
import org.apache.hadoop.hive.serde2.fast.DeserializeRead;
import org.apache.hadoop.hive.serde2.fast.SerializeWrite;
import org.apache.hadoop.hive.serde2.lazybinary.fast.LazyBinaryDeserializeRead;
import org.apache.hadoop.hive.serde2.lazybinary.fast.LazyBinarySerializeWrite;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.variant.Variant;
import org.apache.hadoop.hive.serde2.variant.VariantBuilder;
import org.apache.hadoop.hive.serde2.variant.VariantEncoding;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestVariantColumnVectors {
  private static final TypeInfo VARIANT = TypeInfoFactory.getVariantTypeInfo();

  private static Variant variant(String json) throws Exception {
    return VariantBuilder.parseJson(json, false);
  }

  private static void assertTwoBinaryStruct(ColumnVector vector) {
    assertTrue(vector instanceof StructColumnVector);
    ColumnVector[] fields = ((StructColumnVector) vector).fields;
    assertEquals(2, fields.length);
    assertTrue(fields[0] instanceof BytesColumnVector);
    assertTrue(fields[1] instanceof BytesColumnVector);
  }

  @Test
  public void testVariantColumnIsAStructOfTwoBinaries() {
    assertTwoBinaryStruct(VectorizedBatchUtil.createColumnVector(TypeInfoFactory.getVariantTypeInfo()));
    // scratch columns are created by type name
    assertTwoBinaryStruct(VectorizedBatchUtil.createColumnVector("variant"));
  }

  @Test
  public void testSetAndGetCopyTheBytes() throws Exception {
    StructColumnVector vector = (StructColumnVector) VectorizedBatchUtil.createColumnVector("variant");
    vector.init(); // by-value sets need the bytes buffers, as after VectorizedRowBatch.reset()
    Variant object = variant("{\"a\":1,\"b\":[true,null]}");
    Variant number = variant("40.60");
    Variant source = new Variant(object.getValue().clone(), object.getMetadata().clone());
    VariantColumnVectors.setVariant(vector, 0, source);
    VariantColumnVectors.setVariant(vector, 1, number);
    assertEquals(number, VariantColumnVectors.getVariant(vector, 1));

    // the set copied the source arrays
    Arrays.fill(source.getMetadata(), (byte) 0);
    Arrays.fill(source.getValue(), (byte) 0);
    Variant read = VariantColumnVectors.getVariant(vector, 0);
    assertEquals(object, read);

    // the get copied the vector's buffers
    for (ColumnVector field : vector.fields) {
      Arrays.fill(((BytesColumnVector) field).vector[0], (byte) 0);
    }
    assertEquals(object, read);
  }

  @Test
  public void testVariantArgumentTypeIsNotStruct() throws Exception {
    assertEquals(VectorExpressionDescriptor.ArgumentType.VARIANT,
        VectorExpressionDescriptor.ArgumentType.fromHiveTypeName("variant"));
    assertEquals("Variant", VectorizationContext.getUndecoratedName("variant"));
    assertEquals(VectorExpressionDescriptor.ArgumentType.VARIANT,
        VectorExpressionDescriptor.ArgumentType.getType("Variant"));
    // generic expressions (IS [NOT] NULL) match it, struct-only ones do not
    assertTrue(VectorExpressionDescriptor.ArgumentType.VARIANT
        .isSameTypeOrFamily(VectorExpressionDescriptor.ArgumentType.ALL_FAMILY));
    assertFalse(VectorExpressionDescriptor.ArgumentType.VARIANT
        .isSameTypeOrFamily(VectorExpressionDescriptor.ArgumentType.STRUCT));
    assertEquals(ColumnVector.Type.STRUCT,
        VectorizationContext.getColumnVectorTypeFromTypeInfo(TypeInfoFactory.getVariantTypeInfo()));
  }

  @Test
  public void testIsNullOverAVariantColumnVectorizesNatively() throws Exception {
    VectorizationContext context = variantContext();
    ExprNodeGenericFuncDesc isNull = new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo,
        new GenericUDFOPNull(), List.of(variantColumn()));
    assertEquals(SelectColumnIsNull.class,
        context.getVectorExpression(isNull, VectorExpressionDescriptor.Mode.FILTER).getClass());
    assertEquals(IsNull.class,
        context.getVectorExpression(isNull, VectorExpressionDescriptor.Mode.PROJECTION).getClass());
  }

  @Test
  public void testFunctionOverAVariantColumnFallsBackToTheAdaptor() throws Exception {
    ExprNodeGenericFuncDesc toJson = new ExprNodeGenericFuncDesc(TypeInfoFactory.stringTypeInfo,
        new GenericUDFToJson(), List.of(variantColumn()));
    VectorExpression expression =
        variantContext().getVectorExpression(toJson, VectorExpressionDescriptor.Mode.PROJECTION);
    assertEquals(VectorUDFAdaptor.class, expression.getClass());
  }

  @Test
  public void testNullVariantConstantVectorizes() throws Exception {
    VectorizationContext context = variantContext();
    ExprNodeConstantDesc nullVariant = new ExprNodeConstantDesc(TypeInfoFactory.getVariantTypeInfo(), null);
    // projected, as in SELECT CAST(NULL AS VARIANT), and as the argument of a function the adaptor evaluates
    VectorExpression constant = context.getVectorExpression(nullVariant, VectorExpressionDescriptor.Mode.PROJECTION);
    VectorExpression toJson = context.getVectorExpression(new ExprNodeGenericFuncDesc(TypeInfoFactory.stringTypeInfo,
        new GenericUDFToJson(), List.of(nullVariant)), VectorExpressionDescriptor.Mode.PROJECTION);

    String[] scratchTypes = context.getScratchColumnTypeNames();
    VectorizedRowBatch batch = new VectorizedRowBatch(1 + scratchTypes.length);
    batch.cols[0] = VectorizedBatchUtil.createColumnVector(TypeInfoFactory.getVariantTypeInfo());
    for (int i = 0; i < scratchTypes.length; i++) {
      batch.cols[1 + i] = VectorizedBatchUtil.createColumnVector(scratchTypes[i]);
    }
    batch.reset();
    StructColumnVector constantColumn = (StructColumnVector) batch.cols[constant.getOutputColumnNum()];
    VariantColumnVectors.setVariant(constantColumn, 0, variant("1"));
    batch.size = 3;
    constant.evaluate(batch);
    toJson.evaluate(batch);

    assertTrue(constantColumn.isRepeating);
    assertTrue(constantColumn.isNull[0]);
    ColumnVector toJsonColumn = batch.cols[toJson.getOutputColumnNum()];
    for (int i = 0; i < batch.size; i++) {
      assertTrue(toJsonColumn.isNull[toJsonColumn.isRepeating ? 0 : i]);
    }
  }

  @Test
  public void testRepeatingVectorsReadTheFirstRow() throws Exception {
    Variant value = variant("[1,\"x\"]");
    StructColumnVector column = (StructColumnVector) VectorizedBatchUtil.createColumnVector("variant");
    column.init();
    VariantColumnVectors.setVariant(column, 0, value);

    // only the children repeat
    column.fields[0].isRepeating = true;
    column.fields[1].isRepeating = true;
    assertEquals(value, VariantColumnVectors.getVariant(column, 5));

    // the whole column repeats: the extraction and the writer both read row 0
    column.isRepeating = true;
    VectorizedRowBatch batch = new VectorizedRowBatch(1);
    batch.cols[0] = column;
    batch.size = 8;
    assertEquals(value, extract(batch, 7));
    VectorExpressionWriter writer = VectorExpressionWriterFactory.genVectorExpressionWritable(
        ObjectInspectorFactory.getStandardVariantObjectInspector());
    assertEquals(value, writer.writeValue(column, 7));

    // a repeating null is null on every row
    column.noNulls = false;
    column.isNull[0] = true;
    assertNull(extract(batch, 7));
    assertNull(writer.writeValue(column, 7));
  }

  @Test
  public void testAssignThenExtractRoundTrip() throws Exception {
    Variant value = variant("[1,\"x\"]");
    VectorizedRowBatch batch = new VectorizedRowBatch(1);
    batch.cols[0] = VectorizedBatchUtil.createColumnVector("variant");
    batch.reset();

    VectorAssignRow assignRow = new VectorAssignRow();
    assignRow.init(List.of("variant"));
    assignRow.assignRowColumn(batch, 0, 0, value);
    assignRow.assignRowColumn(batch, 1, 0, null);
    batch.size = 2;

    StructColumnVector column = (StructColumnVector) batch.cols[0];
    assertTrue(column.isNull[1]);
    // a null variant nulls its children, as a null struct does
    assertTrue(column.fields[0].isNull[1]);
    assertTrue(column.fields[1].isNull[1]);

    assertEquals(value, extract(batch, 0));
    assertNull(extract(batch, 1));
  }

  @Test
  public void testVariantAssignedOverAnEarlierNullOfTheSameRowIsNotNull() throws Exception {
    Variant value = variant("{\"a\":[1,null]}");
    VectorizedRowBatch batch = new VectorizedRowBatch(1);
    batch.cols[0] = VectorizedBatchUtil.createColumnVector("variant");
    batch.reset();

    // a scratch column is refilled without a reset: the second batch reuses the row the first one left null
    VectorAssignRow assignRow = new VectorAssignRow();
    assignRow.init(List.of("variant"));
    assignRow.assignRowColumn(batch, 0, 0, null);
    assignRow.assignRowColumn(batch, 0, 0, value);
    batch.size = 1;

    StructColumnVector column = (StructColumnVector) batch.cols[0];
    assertFalse(column.isNull[0]);
    assertFalse(column.fields[0].isNull[0]);
    assertFalse(column.fields[1].isNull[0]);

    assertEquals(value, extract(batch, 0));

    byte[] bytes = serialize(new LazyBinarySerializeWrite(1), batch, VARIANT);
    assertArrayEquals(serialize(new LazyBinarySerializeWrite(1), VARIANT, value), bytes);
    assertEquals(value, deserializeOne(bytes));
  }

  @Test
  public void testExpressionWriterReturnsStandardVariants() throws Exception {
    Variant value = variant("{\"k\":\"v\"}");
    StructColumnVector column = (StructColumnVector) VectorizedBatchUtil.createColumnVector("variant");
    column.init();
    VariantColumnVectors.setVariant(column, 0, value);
    column.noNulls = false;
    column.isNull[1] = true;

    VectorExpressionWriter writer = VectorExpressionWriterFactory.genVectorExpressionWritable(
        ObjectInspectorFactory.getStandardVariantObjectInspector());
    assertEquals(ObjectInspector.Category.VARIANT, writer.getObjectInspector().getCategory());
    assertEquals(value, writer.writeValue(column, 0));
    assertNull(writer.writeValue(column, 1));
  }

  @Test
  public void testVectorSerializationWritesTheStructBytes() throws Exception {
    Variant value = variant("{\"k\":[1,2]}");
    assertArrayEquals(serialize(new LazyBinarySerializeWrite(1), VariantEncoding.TYPE_INFO, value),
        serialize(new LazyBinarySerializeWrite(1), VARIANT, value));
    assertArrayEquals(serialize(new BinarySortableSerializeWrite(1), VariantEncoding.TYPE_INFO, value),
        serialize(new BinarySortableSerializeWrite(1), VARIANT, value));
  }

  @Test
  public void testLazyBinaryVectorRoundTrip() throws Exception {
    Variant value = variant("{\"k\":[1,2]}");
    assertEquals(value, deserializeOne(serialize(new LazyBinarySerializeWrite(1), VARIANT, value)));
  }

  @Test
  public void testNonProjectedVariantIsSkippedAndTheNextColumnStillReads() throws Exception {
    Variant value = variant("{\"k\":[1,2]}");
    TypeInfo[] typeInfos = {VARIANT, TypeInfoFactory.intTypeInfo};
    boolean[] columnsToInclude = {false, true};

    VectorizedRowBatch source = new VectorizedRowBatch(2);
    source.cols[0] = VectorizedBatchUtil.createColumnVector("variant");
    source.cols[1] = VectorizedBatchUtil.createColumnVector("int");
    source.reset();
    VariantColumnVectors.setVariant((StructColumnVector) source.cols[0], 0, value);
    ((LongColumnVector) source.cols[1]).vector[0] = 42;
    source.size = 1;

    DeserializeRead[] reads = {
        new LazyBinaryDeserializeRead(typeInfos, false),
        new BinarySortableDeserializeRead(typeInfos, false, new boolean[2],
            new byte[] {BinarySortableSerDe.ZERO, BinarySortableSerDe.ZERO},
            new byte[] {BinarySortableSerDe.ONE, BinarySortableSerDe.ONE})};
    SerializeWrite[] writes = {new LazyBinarySerializeWrite(2), new BinarySortableSerializeWrite(2)};
    for (int i = 0; i < reads.length; i++) {
      byte[] bytes = serialize(writes[i], source, typeInfos);

      VectorDeserializeRow<DeserializeRead> deserializeRow = new VectorDeserializeRow<>(reads[i]);
      deserializeRow.init(columnsToInclude);
      VectorizedRowBatch target = new VectorizedRowBatch(2);
      target.cols[0] = VectorizedBatchUtil.createColumnVector("variant");
      target.cols[1] = VectorizedBatchUtil.createColumnVector("int");
      target.reset();
      deserializeRow.setBytes(bytes, 0, bytes.length);
      deserializeRow.deserialize(target, 0);

      assertEquals(reads[i].getClass().getSimpleName(), 42, ((LongColumnVector) target.cols[1]).vector[0]);
      assertTrue(target.cols[1].noNulls);
    }
  }

  private static ExprNodeColumnDesc variantColumn() {
    return new ExprNodeColumnDesc(TypeInfoFactory.getVariantTypeInfo(), "v", "t", false);
  }

  private static VectorizationContext variantContext() {
    return new VectorizationContext("name", List.of("v"), List.of(TypeInfoFactory.getVariantTypeInfo()),
        List.of(DataTypePhysicalVariation.NONE), new HiveConf());
  }

  /** Serializes one row holding the variant in a column of the given type; both types share the vector layout. */
  private static byte[] serialize(SerializeWrite serializeWrite, TypeInfo type, Variant value) throws Exception {
    VectorizedRowBatch batch = new VectorizedRowBatch(1);
    batch.cols[0] = VectorizedBatchUtil.createColumnVector(type);
    batch.reset();
    VariantColumnVectors.setVariant((StructColumnVector) batch.cols[0], 0, value);
    batch.size = 1;
    return serialize(serializeWrite, batch, type);
  }

  /** Serializes row 0 of the batch, whose columns have the given types. */
  private static byte[] serialize(SerializeWrite serializeWrite, VectorizedRowBatch batch, TypeInfo... types)
      throws Exception {
    VectorSerializeRow<SerializeWrite> serializeRow = new VectorSerializeRow<>(serializeWrite);
    serializeRow.init(types);
    Output output = new Output();
    serializeRow.setOutput(output);
    serializeRow.serializeWrite(batch, 0);
    return Arrays.copyOf(output.getData(), output.getLength());
  }

  /** Reads a row of one LazyBinary variant into a new batch, and returns the variant. */
  private static Variant deserializeOne(byte[] bytes) throws Exception {
    VectorDeserializeRow<LazyBinaryDeserializeRead> deserializeRow =
        new VectorDeserializeRow<>(new LazyBinaryDeserializeRead(new TypeInfo[] {VARIANT}, false));
    deserializeRow.init();
    VectorizedRowBatch batch = new VectorizedRowBatch(1);
    batch.cols[0] = VectorizedBatchUtil.createColumnVector(VARIANT);
    batch.reset();
    deserializeRow.setBytes(bytes, 0, bytes.length);
    deserializeRow.deserialize(batch, 0);
    return VariantColumnVectors.getVariant((StructColumnVector) batch.cols[0], 0);
  }

  /** The row of a one-column variant batch, as the row-mode operators get it. */
  private static Object extract(VectorizedRowBatch batch, int row) throws Exception {
    VectorExtractRow extractRow = new VectorExtractRow();
    extractRow.init(new TypeInfo[] {VARIANT});
    Object[] values = new Object[1];
    extractRow.extractRow(batch, row, values);
    return values[0];
  }
}
