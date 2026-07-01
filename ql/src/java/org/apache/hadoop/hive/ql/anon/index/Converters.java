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

package org.apache.hadoop.hive.ql.anon.index;

import org.apache.hadoop.hive.ql.anon.ex.BtreeException;
import org.apache.hadoop.hive.ql.anon.index.api.BtreeDataEntry;
import org.apache.hadoop.hive.ql.anon.index.api.BtreeIndexEntry;
import org.apache.hadoop.hive.ql.anon.index.api.BtreePageAddress;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.hadoop.hive.ql.anon.consts.BtreeConst.*;

public class Converters {

  private Converters() {
  }

  public static void convert(Object o, DataOutput out) throws IOException {
    byte[] ret;
    if (o instanceof IntWritable) {
      ByteBuffer bb = ByteBuffer.allocate(4);
      bb.putInt(((IntWritable) o).get());
      ret = bb.array();
    } else {
      throw new BtreeException("convert");
    }

    out.writeInt(ret.length);
    out.write(ret);
  }

  public static Object convert(DataInput in) throws IOException {
    int dataSize = in.readInt();
    byte[] bytes = new byte[dataSize];
    in.readFully(bytes);
    return bytes;
  }

  public static RawDataEntry convert(BtreeDataEntry dataEntry) {
    return new RawDataEntry(dataEntry.getRawKey(), dataEntry.getRawValue());
  }

  public static byte[] convertWritableToBytes(Writable writable) {
    if (writable instanceof IntWritable) {
      ByteBuffer bb = ByteBuffer.allocate(4);
      bb.putInt(((IntWritable) writable).get());
      return bb.array();
    } else if (writable instanceof LongWritable) {
      ByteBuffer bb = ByteBuffer.allocate(8);
      bb.putLong(((LongWritable) writable).get());
      return bb.array();
    } else if (writable instanceof Text) {
      String text = writable.toString();
      int textLength = text.length();
      ByteBuffer bb = ByteBuffer.allocate(4 + textLength);
      bb.putInt(textLength);
      bb.put(text.getBytes());
      return bb.array();
    } else if (writable instanceof BtreePageAddress) {
      BtreePageAddress pageAddress = (BtreePageAddress) writable;
      ByteBuffer bb = ByteBuffer.allocate(4);
      bb.putInt(pageAddress.getPageId());
      return bb.array();
    }
    throw new BtreeException("convert");
  }

  public static byte[] convert(Writable writable) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(baos);
    writable.write(out);
    out.flush();
    out.close();
    return baos.toByteArray();
  }

  public static RawIndexEntry convert(BtreeIndexEntry indexEntry) {
    return new RawIndexEntry(indexEntry.getRawKey(), indexEntry.getRawValue());
  }

  public static void validateType(byte type) {
    switch (type) {
      case BTREE_BYTE_TYPE:
      case BTREE_SHORT_TYPE:
      case BTREE_INT_TYPE:
      case BTREE_LONG_TYPE:
      case BTREE_TEXT_TYPE:
      case BTREE_BINARY_TYPE:
        return;
      default:
        throw new BtreeException("unsupported type");
    }
  }

  public static void validatePagesSectionStart(byte b) {
    if (b != BTREE_PAGES_SECTION_START) {
      throw new BtreeException("page section start not found");
    }
  }

  public static void validateFileMagicWord(short word) {
    if (word != BTREE_CONF_FILE_MAGIC_VALUE) {
      throw new BtreeException("file magic");
    }
  }
}
