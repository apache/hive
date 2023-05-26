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
package org.apache.hadoop.hive.ql.util.bitmap;

import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class RoaringBitmapSerDe {

  public static ByteBuffer serialize(RoaringBitmap bitmap) {
    int size = -1;
    if (bitmap.getCardinality() <= 32) {
      size = 2 + 4 * bitmap.getCardinality();
    } else {
      int varIntLen = VarInt.varIntSize(bitmap.serializedSizeInBytes());
      size = 1 + varIntLen + bitmap.serializedSizeInBytes();
    }

    ByteBuffer bos = ByteBuffer.allocate(size);
    if (!bos.order().equals(ByteOrder.LITTLE_ENDIAN))
      bos = bos.slice().order(ByteOrder.LITTLE_ENDIAN);

    if (bitmap.getCardinality() <= 32) {
      bos.put(new Integer(0).byteValue());
      bos.put((byte) bitmap.getCardinality());
      for (int e : bitmap.toArray()) {
        bos.putInt(e);
      }
    } else {
      bos.put(new Integer(1).byteValue());
      VarInt.putVarInt(bitmap.serializedSizeInBytes(), bos);
      bitmap.serialize(bos);
    }

    return bos;
  }

  public static RoaringBitmap deserialize(byte[] bytes) throws IOException {
    ByteBuffer input = ByteBuffer.wrap(bytes);
    if (!input.order().equals(ByteOrder.LITTLE_ENDIAN))
      input = input.slice().order(ByteOrder.LITTLE_ENDIAN);

    int flag = input.get();
    if (flag == 0) {
      int num = input.get();
      RoaringBitmap bitmap = new RoaringBitmap();
      for (int i = 0; i < num; i++) {
        bitmap.add(input.getInt());
      }
      return bitmap;
    } else {
      int size = VarInt.getVarInt(input);
      RoaringBitmap bitmap = new RoaringBitmap();
      bitmap.deserialize(input);
      assert (bitmap.serializedSizeInBytes() == size);
      return bitmap;
    }
  }

}