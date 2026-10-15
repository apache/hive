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
package org.apache.hadoop.hive.ql.io.orc;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.io.AbstractTestVariantFileFormat;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructEncodedVariantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.VariantObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.variant.Variant;
import org.apache.hadoop.hive.serde2.variant.VariantBuilder;
import org.apache.hadoop.hive.serde2.variant.VariantEncoding;
import org.apache.hadoop.io.NullWritable;
import org.apache.orc.TypeDescription;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestOrcVariant extends AbstractTestVariantFileFormat {

  private static final String STRUCT_ROW = "struct<id:int,v:" + ENCODING + ",vs:array<" + ENCODING + ">>";

  @Override
  protected String extension() {
    return "orc";
  }

  @Override
  protected void writeRows(Path path, String columnTypes, StructObjectInspector oi, List<List<Object>> rows)
      throws Exception {
    Writer writer = OrcFile.createWriter(path, OrcFile.writerOptions(new Configuration()).inspector(oi));
    for (List<Object> row : rows) {
      writer.addRow(row);
    }
    writer.close();
  }

  @Override
  protected void forEachRow(Path path, String columnNames, String columnTypes, RowConsumer consumer)
      throws Exception {
    Reader reader = OrcFile.createReader(path, OrcFile.readerOptions(new Configuration()));
    StructObjectInspector oi = (StructObjectInspector) OrcStruct.createObjectInspector(
        TypeInfoFactory.getStructTypeInfo(Arrays.asList(columnNames.split(",")),
            TypeInfoUtils.getTypeInfosFromTypeString(columnTypes)));
    RecordReader rows = reader.rows();
    Object row = null;
    while (rows.hasNext()) {
      row = rows.next(row);
      consumer.accept(oi, row);
    }
    rows.close();
  }

  @Override
  protected void assertSameFileSchema(Path variantFile, Path structFile) throws Exception {
    Configuration conf = new Configuration();
    TypeDescription variantSchema = OrcFile.createReader(variantFile, OrcFile.readerOptions(conf)).getSchema();
    TypeDescription structSchema = OrcFile.createReader(structFile, OrcFile.readerOptions(conf)).getSchema();
    assertEquals(STRUCT_ROW, variantSchema.toString());
    assertEquals(structSchema.toString(), variantSchema.toString());
    assertEquals(structSchema, variantSchema);
  }

  @Test
  public void testVariantSchemaIsTheTwoBinaryStruct() {
    assertEquals(ENCODING, OrcInputFormat.convertTypeInfo(TypeInfoFactory.getVariantTypeInfo()).toString());
    assertEquals(OrcInputFormat.convertTypeInfo(VariantEncoding.TYPE_INFO),
        OrcInputFormat.convertTypeInfo(TypeInfoFactory.getVariantTypeInfo()));
    assertEquals(
        OrcInputFormat.convertTypeInfo(TypeInfoUtils.getTypeInfoFromTypeString("map<string,array<" + ENCODING + ">>")),
        OrcInputFormat.convertTypeInfo(TypeInfoUtils.getTypeInfoFromTypeString("map<string,array<variant>>")));
  }

  @Test
  public void testRowInspectorOfVariantIsTheEncodedStruct() {
    ObjectInspector oi = OrcStruct.createObjectInspector(TypeInfoFactory.getVariantTypeInfo());
    assertTrue(oi instanceof StructEncodedVariantObjectInspector);
    assertEquals(ObjectInspector.Category.VARIANT, oi.getCategory());
    assertEquals("variant", oi.getTypeName());
  }

  /** The reader reuses the row and its binaries, so a Variant handed out earlier must not change. */
  @Test
  public void testBatchToRowReaderReuseKeepsEarlierVariants() throws Exception {
    Variant longer =
        VariantBuilder.parseJson("{\"alpha\":[1,2,3,4,5,6,7,8,9],\"beta\":\"a long string value\"}", false);
    Variant shorter = VariantBuilder.parseJson("7", false);
    StructObjectInspector rowOI = ObjectInspectorFactory.getStandardStructObjectInspector(
        Arrays.asList("v"),
        Arrays.<ObjectInspector>asList(ObjectInspectorFactory.getStandardVariantObjectInspector()));
    VectorizedRowBatchCtx ctx = new VectorizedRowBatchCtx();
    ctx.init(rowOI, new String[0]);

    StructColumnVector column = new StructColumnVector(VectorizedRowBatch.DEFAULT_SIZE,
        new BytesColumnVector(), new BytesColumnVector());
    int row = 0;
    for (Variant variant : Arrays.asList(longer, shorter)) {
      ((BytesColumnVector) column.fields[0]).setRef(row, variant.getMetadata(), 0, variant.getMetadata().length);
      ((BytesColumnVector) column.fields[1]).setRef(row, variant.getValue(), 0, variant.getValue().length);
      row++;
    }
    column.noNulls = false;
    column.isNull[2] = true;
    VectorizedRowBatch batch = new VectorizedRowBatch(1);
    batch.cols[0] = column;
    batch.size = 3;

    OrcOiBatchToRowReader reader = new OrcOiBatchToRowReader(new SingleBatchReader(batch), ctx, null);
    StructObjectInspector readOI = (StructObjectInspector) OrcStruct.createObjectInspector(
        TypeInfoUtils.getTypeInfoFromTypeString("struct<v:variant>"));
    StructField field = readOI.getStructFieldRef("v");
    VariantObjectInspector variantOI = (VariantObjectInspector) field.getFieldObjectInspector();

    Object value = reader.createValue();
    assertTrue(reader.next(NullWritable.get(), value));
    Variant first = variantOI.getVariant(readOI.getStructFieldData(value, field));
    assertTrue(reader.next(NullWritable.get(), value));
    Variant second = variantOI.getVariant(readOI.getStructFieldData(value, field));
    assertTrue(reader.next(NullWritable.get(), value));
    Variant third = variantOI.getVariant(readOI.getStructFieldData(value, field));

    assertEquals(longer, first);
    assertEquals(shorter, second);
    assertNull(third);
  }

  /** Hands out one prepared batch. */
  private static final class SingleBatchReader
      implements org.apache.hadoop.mapred.RecordReader<NullWritable, VectorizedRowBatch> {
    private final VectorizedRowBatch batch;
    private boolean done;

    SingleBatchReader(VectorizedRowBatch batch) {
      this.batch = batch;
    }

    @Override
    public boolean next(NullWritable key, VectorizedRowBatch value) {
      if (done) {
        return false;
      }
      done = true;
      return true;
    }

    @Override
    public NullWritable createKey() {
      return NullWritable.get();
    }

    @Override
    public VectorizedRowBatch createValue() {
      return batch;
    }

    @Override
    public long getPos() {
      return 0;
    }

    @Override
    public void close() {
    }

    @Override
    public float getProgress() {
      return 0;
    }
  }
}
