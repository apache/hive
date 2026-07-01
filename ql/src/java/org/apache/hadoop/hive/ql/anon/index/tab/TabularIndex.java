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

package org.apache.hadoop.hive.ql.anon.index.tab;

import org.apache.hadoop.conf.Configuration;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import static org.apache.hadoop.hive.ql.anon.consts.BtreeConst.*;

public class TabularIndex {

  private Configuration conf;

  private String keyType;
  private String valueTypes;

  private int _100MB = 100 * 1024 * 1024;

  private final ByteArrayOutputStream baos = new ByteArrayOutputStream(_100MB);
  private final DataOutputStream dos = new DataOutputStream(baos);

  private int keyCount = 0;

  public TabularIndex(final Configuration conf) {
    this.conf = conf;

    keyType = conf.get(INDEX_KEY_TYPE);
    valueTypes = conf.get(INDEX_VALUE_TYPES);

    if (keyType.length() != 1) {
      throw new RuntimeException("keyType must be 1 byte");
    }
    if (valueTypes.length() != 3) {
      throw new RuntimeException("valueTypes must be 3 bytes");
    }
  }

  public void addEntry(final TabularEntry entry) throws IOException {
    entry.write(dos);
    keyCount++;
  }

  public void save(final OutputStream os) throws IOException {
    final byte[] baKeys = baos.toByteArray();
    final int baKeysLength = baKeys.length;
    final byte[] header = prepareHeader(baKeysLength);
    os.write(header);
    os.write(baKeys);
  }

  private byte[] prepareHeader(final int baKeysLength) {
    final ByteBuffer bb = ByteBuffer.allocate(INDEX_HEADER_SIZE);
    bb.putShort(TABULAR_CONF_FILE_MAGIC_VALUE);

    bb.put((byte) keyType.charAt(0));
    for (int i = 0; i < valueTypes.length(); i++) {
      bb.put((byte) valueTypes.charAt(i));
    }

    bb.putInt(keyCount);
    bb.putInt(baKeysLength);

    return bb.array();
  }

}
