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
package org.apache.hadoop.hive.ql.io.parquet.vector;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.time.DateTimeException;
import java.util.Arrays;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.junit.Test;

public class TestDictionaryValues {

  /**
   * A dictionary entry whose conversion fails (an INT96 date the proleptic calendar rejects, read as a string) fails
   * only the rows that read it.
   */
  @Test
  public void testFailingEntryFailsOnlyItsRows() {
    DateTimeException error = new DateTimeException("Invalid date 'February 29' as '1500' is not a leap year");
    Dictionary dictionary = mock(Dictionary.class);
    when(dictionary.getMaxId()).thenReturn(2);
    ParquetDataColumnReader reader = mock(ParquetDataColumnReader.class);
    when(reader.getDictionary()).thenReturn(dictionary);
    when(reader.readString(0)).thenReturn(bytes("a"));
    when(reader.readString(1)).thenThrow(error);
    when(reader.readString(2)).thenReturn(bytes("c"));
    DictionaryValues values = new DictionaryValues.Bytes(reader,
        Types.required(PrimitiveTypeName.INT96).named("ts"), TypeInfoFactory.stringTypeInfo);

    BytesColumnVector column = new BytesColumnVector(2);
    values.decodeDictionaryIds(2, 2, 0, column, new int[] { 0, 2 });
    assertArrayEquals(bytes("a"), row(column, 0));
    assertArrayEquals(bytes("c"), row(column, 1));

    assertSame(error, assertThrows(DateTimeException.class,
        () -> values.decodeDictionaryIds(1, 1, 0, new BytesColumnVector(1), new int[] { 1 })));
  }

  private static byte[] bytes(String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }

  private static byte[] row(BytesColumnVector column, int row) {
    return Arrays.copyOfRange(column.vector[row], column.start[row], column.start[row] + column.length[row]);
  }
}
