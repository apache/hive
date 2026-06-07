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

package org.apache.hadoop.hive.ql.anon.index.impl;

import org.apache.hadoop.hive.ql.anon.index.Converters;
import org.apache.hadoop.hive.ql.anon.index.api.BtreeIndexEntry;
import org.apache.hadoop.hive.ql.anon.index.api.BtreeKey;
import org.apache.hadoop.hive.ql.anon.index.api.BtreePageAddress;
import org.apache.hadoop.hive.ql.anon.index.api.BtreeValue;
import org.apache.hadoop.io.IntWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class BtreeIndexEntryImpl implements BtreeIndexEntry {

  private final byte[] rawKey;
  private final byte[] rawValue;

  public BtreeIndexEntryImpl(byte[] key, BtreePageAddress pageAddress) {
    this.rawKey = key;
    this.rawValue = Converters.convertWritableToBytes(pageAddress);
  }

  public BtreeIndexEntryImpl(byte[] key, int pageId) throws IOException {
    this.rawKey = key;

    IntWritable iwPageId = new IntWritable(pageId);
    this.rawValue = Converters.convert(iwPageId);
  }

  private byte[] data;

  @Override
  public void write(DataOutput out) throws IOException {

  }

  @Override
  public void readFields(DataInput in) throws IOException {

  }

  @Override
  public BtreeKey getKey() {
    return null;
  }

  @Override
  public BtreeValue getValue() {
    return null;
  }

  @Override
  public byte[] getRawKey() {
    return rawKey;
  }

  @Override
  public byte[] getRawValue() {
    return rawValue;
  }

  @Override
  public int getSize() {
    return 0;
  }

  @Override
  public byte[] getBytes() throws IOException {
    return data;
  }
}
