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
package org.apache.hadoop.hive.ql.io;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.VariantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.variant.Variant;
import org.apache.hadoop.hive.serde2.variant.VariantBuilder;
import org.apache.hadoop.hive.serde2.variant.VariantEncoding;
import org.apache.hadoop.io.BytesWritable;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * A file format stores a variant as the struct of its two binaries: rows of (id, v, vs) typed variant and typed as that
 * struct give the same file, and either file reads back as either type. This class builds and decodes the struct
 * values itself, so they do not depend on the code under test.
 */
public abstract class AbstractTestVariantFileFormat {

  protected static final String ENCODING = VariantEncoding.TYPE_INFO.getTypeName();
  protected static final String COLUMNS = "id,v,vs";
  protected static final String VARIANT_COLUMNS = "int,variant,array<variant>";
  protected static final String STRUCT_COLUMNS = "int," + ENCODING + ",array<" + ENCODING + ">";

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  /** Receives each row a reader returns, with the inspector of the row. */
  @FunctionalInterface
  protected interface RowConsumer {
    void accept(StructObjectInspector oi, Object row);
  }

  protected abstract String extension();

  /** Writes the rows of columns (id, v, vs), typed {@code columnTypes}, through the inspector given. */
  protected abstract void writeRows(Path path, String columnTypes, StructObjectInspector oi, List<List<Object>> rows)
      throws Exception;

  /** Reads every row of {@code path} with the columns named and typed as given; the row may be reused. */
  protected abstract void forEachRow(Path path, String columnNames, String columnTypes, RowConsumer consumer)
      throws Exception;

  /** Asserts that the file written typed variant has the schema of the file written typed as the struct. */
  protected abstract void assertSameFileSchema(Path variantFile, Path structFile) throws Exception;

  @Test
  public void testSqlNullAndJsonNullStayDistinct() throws Exception {
    Variant jsonNull = VariantBuilder.parseJson("null", false);
    List<List<Variant>> rows = Arrays.asList(
        Arrays.asList((Variant) null, jsonNull), Arrays.asList(jsonNull, (Variant) null));
    Path path = path("nulls");

    write(path, true, rows);

    List<List<Variant>> read = readVariants(path, COLUMNS, VARIANT_COLUMNS);
    assertEquals(rows, read);
    assertNull(read.get(0).get(0));
    assertNotNull(read.get(0).get(1));
  }

  @Test
  public void testVariantFileIsTheStructFile() throws Exception {
    List<List<Variant>> rows = Arrays.asList(
        Arrays.asList(VariantBuilder.parseJson("{\"a\":1}", false),
            VariantBuilder.parseJson("[1,\"x\"]", false), null, VariantBuilder.parseJson("null", false)),
        Arrays.asList((Variant) null, (Variant) null),
        Arrays.asList(VariantBuilder.parseJson("40.60", false),
            VariantBuilder.parseJson("{\"alpha\":{\"beta\":[true,null,\"long enough text\"]}}", false)));
    Path variantFile = path("typed");
    Path structFile = path("plain");

    write(variantFile, true, rows);
    write(structFile, false, rows);

    assertSameFileSchema(variantFile, structFile);
    // each file read as variant, and each file read as the struct, hold the same values
    assertEquals(rows, readVariants(variantFile, COLUMNS, VARIANT_COLUMNS));
    assertEquals(rows, readVariants(structFile, COLUMNS, VARIANT_COLUMNS));
    assertEquals(rows, readVariants(variantFile, COLUMNS, STRUCT_COLUMNS));
    assertEquals(rows, readVariants(structFile, COLUMNS, STRUCT_COLUMNS));
  }

  protected Path path(String name) {
    return new Path(folder.getRoot().getPath(), name + "." + extension());
  }

  /**
   * Writes rows of (id, v, vs) typed variant, or typed as the two-binary struct; the id is the row number. Each row
   * holds v, then the elements of vs.
   */
  protected void write(Path path, boolean variantTyped, List<List<Variant>> rows) throws Exception {
    ObjectInspector element = variantTyped ? ObjectInspectorFactory.getStandardVariantObjectInspector()
        : TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(TypeInfoUtils.getTypeInfoFromTypeString(ENCODING));
    StructObjectInspector oi = ObjectInspectorFactory.getStandardStructObjectInspector(
        Arrays.asList("id", "v", "vs"),
        Arrays.asList(PrimitiveObjectInspectorFactory.javaIntObjectInspector, element,
            ObjectInspectorFactory.getStandardListObjectInspector(element)));
    List<List<Object>> written = new ArrayList<>();
    for (int i = 0; i < rows.size(); i++) {
      List<Variant> variants = rows.get(i);
      List<Object> list = new ArrayList<>();
      for (Variant variant : variants.subList(1, variants.size())) {
        list.add(variantTyped ? variant : toStructValue(variant));
      }
      Variant first = variants.get(0);
      written.add(Arrays.<Object>asList(i, variantTyped ? first : toStructValue(first), list));
    }
    writeRows(path, variantTyped ? VARIANT_COLUMNS : STRUCT_COLUMNS, oi, written);
  }

  private static List<byte[]> toStructValue(Variant variant) {
    return variant == null ? null : Arrays.asList(variant.getMetadata(), variant.getValue());
  }

  /**
   * Reads the variants of every row of {@code path}, with the columns named and typed as given: each variant column,
   * and the elements of each array column, in order. Primitive columns are skipped.
   */
  protected List<List<Variant>> readVariants(Path path, String columnNames, String columnTypes) throws Exception {
    List<List<Variant>> result = new ArrayList<>();
    forEachRow(path, columnNames, columnTypes, (oi, row) -> {
      List<Variant> variants = new ArrayList<>();
      for (StructField field : oi.getAllStructFieldRefs()) {
        ObjectInspector fieldOI = field.getFieldObjectInspector();
        Object data = oi.getStructFieldData(row, field);
        if (fieldOI instanceof ListObjectInspector listOI) {
          for (int i = 0; i < listOI.getListLength(data); i++) {
            variants.add(variantOf(listOI.getListElementObjectInspector(), listOI.getListElement(data, i)));
          }
        } else if (fieldOI.getCategory() != ObjectInspector.Category.PRIMITIVE) {
          variants.add(variantOf(fieldOI, data));
        }
      }
      result.add(variants);
    });
    return result;
  }

  /** The variant of a value typed variant, or of one typed as the two-binary struct. */
  private static Variant variantOf(ObjectInspector oi, Object data) {
    if (oi instanceof VariantObjectInspector) {
      return ((VariantObjectInspector) oi).getVariant(data);
    }
    if (data == null) {
      return null;
    }
    StructObjectInspector struct = (StructObjectInspector) oi;
    BytesWritable metadata = (BytesWritable) struct.getStructFieldData(data, struct.getAllStructFieldRefs().get(0));
    BytesWritable value = (BytesWritable) struct.getStructFieldData(data, struct.getAllStructFieldRefs().get(1));
    return new Variant(Arrays.copyOf(value.getBytes(), value.getLength()),
        Arrays.copyOf(metadata.getBytes(), metadata.getLength()));
  }
}
