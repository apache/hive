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

package org.apache.iceberg.deletes;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.zip.CRC32;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;

public class TestDeletionVectors {
  private static final DeleteFile DELETE_FILE = FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
      .ofPositionDeletes()
      .withPath("file:/tmp/data/deletes.parquet")
      .withFileSizeInBytes(100)
      .withRecordCount(10)
      .build();

  @Test
  public void testRoundTrip() {
    PositionDeleteIndex index = DeletionVectors.deserialize(serialize(1, 5, 70_000, 5_000_000_000L), DELETE_FILE);

    Assert.assertEquals(4, index.cardinality());
    for (long position : new long[] { 1, 5, 70_000, 5_000_000_000L }) {
      Assert.assertTrue(index.isDeleted(position));
    }
    Assert.assertEquals(ImmutableList.of(DELETE_FILE), ImmutableList.copyOf(index.deleteFiles()));
  }

  @Test
  public void testCorruptedBitmapFailsTheChecksum() {
    byte[] bytes = serialize(1, 5);
    bytes[bytes.length - 5]++;

    assertInvalid(bytes, "Invalid CRC");
  }

  @Test
  public void testWrongMagicNumber() {
    byte[] bytes = serialize(1, 5);
    bytes[Integer.BYTES]++;
    CRC32 crc = new CRC32();
    crc.update(bytes, Integer.BYTES, bytes.length - 2 * Integer.BYTES);
    ByteBuffer.wrap(bytes).putInt(bytes.length - Integer.BYTES, (int) crc.getValue());

    assertInvalid(bytes, "Invalid magic number");
  }

  @Test
  public void testWrongLength() {
    byte[] bytes = serialize(1, 5);
    ByteBuffer.wrap(bytes).putInt(0, bytes.length);

    assertInvalid(bytes, "Invalid bitmap data length");
  }

  private static byte[] serialize(long... positions) {
    return Deletes.toPositionIndex(CloseableIterable.withNoopClose(Arrays.stream(positions).boxed().toList()))
        .serialize().array();
  }

  private static void assertInvalid(byte[] bytes, String message) {
    IllegalArgumentException error = Assert.assertThrows(IllegalArgumentException.class,
        () -> DeletionVectors.deserialize(bytes, DELETE_FILE));
    Assert.assertTrue(error.getMessage(), error.getMessage().startsWith(message));
  }
}
