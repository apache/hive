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

package org.apache.hadoop.hive.ql.anon.index.dir;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.anon.index.RawDataEntry;
import org.apache.hadoop.io.IntWritable;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import static org.apache.hadoop.hive.ql.anon.consts.BtreeConst.*;
import static org.apache.hadoop.hive.ql.anon.index.Converters.convert;

public class DirectoryIndex {

  private final Configuration conf;

  private final String keyType;
  private final String pointerType;
  private final String valueTypes;

  private int rawKeysLength = 0;
  private int rawPointersLength = 0;
  private int pointerOffset = 0;
  private int valueOffset = 0;
  private int _100MB = 100 * 1024 * 1024;
  private final ByteArrayOutputStream baosKeys = new ByteArrayOutputStream(_100MB);
  private final DataOutputStream dosKeys = new DataOutputStream(baosKeys);
  private final ByteArrayOutputStream baosPointers = new ByteArrayOutputStream(_100MB);
  private final DataOutputStream dosPointers = new DataOutputStream(baosPointers);
  private final ByteArrayOutputStream baosValues = new ByteArrayOutputStream(_100MB);
  private final DataOutputStream dosValues = new DataOutputStream(baosValues);

  private int keyCount = 0;

  public DirectoryIndex(final Configuration conf) {
    this.conf = conf;
    keyType = conf.get(INDEX_KEY_TYPE);
    valueTypes = conf.get(INDEX_VALUE_TYPES);
    pointerType = conf.get(INDEX_ADDR_TYPE);

    if (keyType.length() != 1) {
      throw new RuntimeException("keyType must be 1 byte");
    }
    if (valueTypes.length() != 3) {
      throw new RuntimeException("valueTypes must be 3 bytes");
    }
    if (pointerType.length() != 1) {
      throw new RuntimeException("pointerType must be 1 byte");
    }
  }

  public void addEntry(final RawDataEntry entry) throws IOException {

    byte[] key = entry.getRawKey();
    byte[] value = entry.getRawValue();
    keyCount++;

    dosKeys.write(key);
    rawKeysLength += key.length;

    byte[] rawPointer = convert(new IntWritable(pointerOffset));
    dosPointers.write(rawPointer);
    dosValues.write(value);
    pointerOffset += value.length;
  }

  public void save(final OutputStream os) throws IOException {

    byte[] baKeys = baosKeys.toByteArray();
    byte[] baPointers = baosPointers.toByteArray();
    byte[] baValues = baosValues.toByteArray();

    int baKeysLength = baKeys.length;
    int baPointersLength = baPointers.length;
    int baValuesLength = baValues.length;

    byte[] header = prepareHeader(baKeysLength, baPointersLength, baValuesLength);
    os.write(header);

    os.write(baKeys);
    os.write(baPointers);
    os.write(baValues);
  }

  private byte[] prepareHeader(final int baKeysLength, final int baPointersLength, final int baValuesLength) {
    ByteBuffer bb = ByteBuffer.allocate(INDEX_HEADER_SIZE);
    bb.putShort(DIRECTORY_CONF_FILE_MAGIC_VALUE);

    bb.put((byte) keyType.charAt(0));
    bb.put((byte) pointerType.charAt(0));
    for (int i = 0; i < valueTypes.length(); i++) {
      bb.put((byte) valueTypes.charAt(i));
    }

    bb.putInt(keyCount);
    bb.putInt(baKeysLength);
    bb.putInt(baPointersLength);
    bb.putInt(baValuesLength);

    return bb.array();
  }

}
