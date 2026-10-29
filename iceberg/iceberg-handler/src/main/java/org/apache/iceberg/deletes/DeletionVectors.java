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
import java.nio.ByteOrder;
import java.util.zip.CRC32;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Iceberg has no public deletion vector deserializer that does not validate against a DV DeleteFile; this can go once
 * it offers one.
 */
public final class DeletionVectors {
  private static final int MAGIC_NUMBER = 0x6439D3D1;

  private DeletionVectors() {
  }

  /**
   * Reads the {@link PositionDeleteIndex#serialize()} format into an index attributed to the given delete file.
   */
  public static PositionDeleteIndex deserialize(byte[] bytes, DeleteFile deleteFile) {
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    int bitmapDataLength = buffer.getInt();
    Preconditions.checkArgument(bitmapDataLength == bytes.length - 2 * Integer.BYTES,
        "Invalid bitmap data length: %s", bitmapDataLength);
    CRC32 crc = new CRC32();
    crc.update(bytes, Integer.BYTES, bitmapDataLength);
    Preconditions.checkArgument((int) crc.getValue() == buffer.getInt(Integer.BYTES + bitmapDataLength),
        "Invalid CRC");
    ByteBuffer bitmapData = ByteBuffer.wrap(bytes, Integer.BYTES, bitmapDataLength).order(ByteOrder.LITTLE_ENDIAN);
    int magicNumber = bitmapData.getInt();
    Preconditions.checkArgument(magicNumber == MAGIC_NUMBER, "Invalid magic number: %s", magicNumber);
    return new BitmapPositionDeleteIndex(RoaringPositionBitmap.deserialize(bitmapData), deleteFile);
  }
}
