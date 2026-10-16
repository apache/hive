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

import org.apache.hadoop.hive.serde2.variant.Variant;
import org.apache.hadoop.hive.serde2.variant.VariantEncoding;

/**
 * A VARIANT column vector: a {@link StructColumnVector} of two {@link BytesColumnVector}s, the metadata and the
 * value binaries in {@link org.apache.hadoop.hive.serde2.variant.VariantEncoding} order. ORC, Parquet and the
 * Iceberg readers produce this layout.
 */
public final class VariantColumnVectors {

  private VariantColumnVectors() {
  }

  /** The variant at a row the caller has checked is not null. The bytes are copied. */
  public static Variant getVariant(StructColumnVector vector, int row) {
    return new Variant(copy((BytesColumnVector) vector.fields[VariantEncoding.VALUE_INDEX], row),
        copy((BytesColumnVector) vector.fields[VariantEncoding.METADATA_INDEX], row));
  }

  public static void setVariant(StructColumnVector vector, int row, Variant variant) {
    byte[] metadata = variant.getMetadata();
    byte[] value = variant.getValue();
    BytesColumnVector metadataVector = (BytesColumnVector) vector.fields[VariantEncoding.METADATA_INDEX];
    BytesColumnVector valueVector = (BytesColumnVector) vector.fields[VariantEncoding.VALUE_INDEX];
    metadataVector.setVal(row, metadata, 0, metadata.length);
    valueVector.setVal(row, value, 0, value.length);
    // the row may hold an earlier null: setVal leaves the null flags alone
    metadataVector.isNull[row] = false;
    valueVector.isNull[row] = false;
  }

  private static byte[] copy(BytesColumnVector bytes, int row) {
    int index = bytes.isRepeating ? 0 : row;
    return Arrays.copyOfRange(bytes.vector[index], bytes.start[index], bytes.start[index] + bytes.length[index]);
  }
}
