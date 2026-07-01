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
import org.apache.hadoop.hive.ql.anon.index.api.BtreeDataEntry;
import org.apache.hadoop.hive.ql.anon.index.api.BtreeKey;
import org.apache.hadoop.hive.ql.anon.index.api.BtreeValue;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class BtreeDataEntryImpl implements BtreeDataEntry {
  private BtreeKey key;
  private BtreeValue value;

  private byte[] rawKey;
  private byte[] rawValue;

  public BtreeDataEntryImpl(WritableComparable key, Writable value) throws IOException {

    this.rawKey = Converters.convert(key);
    this.rawValue = Converters.convert(value);
  }

  public BtreeDataEntryImpl() {

  }

  @Override
  public void write(DataOutput out) throws IOException {

  }

  @Override
  public void readFields(DataInput in) throws IOException {

  }

  @Override
  public BtreeKey getKey() {
    return new BtreeKeyImpl(key);
  }

  @Override
  public BtreeValue getValue() {
    return new BtreeValueImpl(value);
  }

  @Override
  public int getSize() throws IOException {
    return rawKey.length + rawValue.length;
  }

  @Override
  public byte[] getBytes() throws IOException {
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

}
