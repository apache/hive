/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.mr.hive.vector;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.iceberg.variants.PhysicalType;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.VariantValue;
import org.junit.Assert;
import org.junit.Test;

public class TestParquetVariantRecordReader {

  @Test
  public void testWriteMetadataUsesPreallocatedBuffer() throws Exception {
    BytesColumnVector vector = new BytesColumnVector(2);
    vector.init(); // allocates the shared buffer used by ensureValPreallocated

    Method writeToVector =
        ParquetVariantRecordReader.class.getDeclaredMethod(
            "writeToVector", BytesColumnVector.class, int.class, VariantMetadata.class);
    writeToVector.setAccessible(true);

    byte[] bytes0 = new byte[] {1, 2, 3};
    byte[] bytes1 = new byte[] {4, 5};

    writeToVector.invoke(null, vector, 0, new FixedBytesMetadata(bytes0));
    writeToVector.invoke(null, vector, 1, new FixedBytesMetadata(bytes1));

    // For small values, BytesColumnVector stores values in a shared internal buffer, not per-row byte arrays.
    Assert.assertSame("Expected shared backing array for both rows", vector.vector[0], vector.vector[1]);
    Assert.assertEquals("Expected first value at offset 0", 0, vector.start[0]);
    Assert.assertEquals("Expected second value to follow the first", bytes0.length, vector.start[1]);

    Assert.assertArrayEquals(bytes0, slice(vector, 0));
    Assert.assertArrayEquals(bytes1, slice(vector, 1));
  }

  @Test
  public void testWriteValueUsesPreallocatedBuffer() throws Exception {
    BytesColumnVector vector = new BytesColumnVector(2);
    vector.init();

    Method writeToVector =
        ParquetVariantRecordReader.class.getDeclaredMethod(
            "writeToVector", BytesColumnVector.class, int.class, VariantValue.class);
    writeToVector.setAccessible(true);

    byte[] bytes0 = new byte[] {9, 8, 7, 6};
    byte[] bytes1 = new byte[] {5};

    writeToVector.invoke(null, vector, 0, new FixedBytesValue(bytes0));
    writeToVector.invoke(null, vector, 1, new FixedBytesValue(bytes1));

    Assert.assertSame("Expected shared backing array for both rows", vector.vector[0], vector.vector[1]);
    Assert.assertEquals(0, vector.start[0]);
    Assert.assertEquals(bytes0.length, vector.start[1]);

    Assert.assertArrayEquals(bytes0, slice(vector, 0));
    Assert.assertArrayEquals(bytes1, slice(vector, 1));
  }

  private static byte[] slice(BytesColumnVector vector, int row) {
    return Arrays.copyOfRange(vector.vector[row], vector.start[row], vector.start[row] + vector.length[row]);
  }

  private static final class FixedBytesMetadata implements VariantMetadata {
    private final byte[] bytes;

    private FixedBytesMetadata(byte[] bytes) {
      this.bytes = bytes;
    }

    @Override
    public int id(String name) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String get(int id) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int dictionarySize() {
      return 0;
    }

    @Override
    public int sizeInBytes() {
      return bytes.length;
    }

    @Override
    public int writeTo(ByteBuffer buffer, int offset) {
      for (int i = 0; i < bytes.length; i++) {
        buffer.put(offset + i, bytes[i]);
      }
      return bytes.length;
    }
  }

  private static final class FixedBytesValue implements VariantValue {
    private final byte[] bytes;

    private FixedBytesValue(byte[] bytes) {
      this.bytes = bytes;
    }

    @Override
    public PhysicalType type() {
      return PhysicalType.BINARY;
    }

    @Override
    public int sizeInBytes() {
      return bytes.length;
    }

    @Override
    public int writeTo(ByteBuffer buffer, int offset) {
      for (int i = 0; i < bytes.length; i++) {
        buffer.put(offset + i, bytes[i]);
      }
      return bytes.length;
    }
  }
}
