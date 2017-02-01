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
package org.apache.hadoop.hive.llap.io.encoded;

import java.io.IOException;

import org.apache.hadoop.hive.llap.io.encoded.SerDeEncodedDataReader.ReaderWithOffsets;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.RecordReader;

@SuppressWarnings("rawtypes") class PassThruOffsetReader implements ReaderWithOffsets {
  protected final RecordReader sourceReader;
  protected final Object key;
  protected final Writable value;

  PassThruOffsetReader(RecordReader sourceReader) {
    this.sourceReader = sourceReader;
    key = sourceReader.createKey();
    value = (Writable)sourceReader.createValue();
  }

  @Override
  public boolean next() throws IOException {
    return sourceReader.next(key, value);
  }

  @Override
  public Writable getCurrentRow() {
    return value;
  }

  @Override
  public void close() throws IOException {
    sourceReader.close();
  }

  @Override
  public long getCurrentRowStartOffset() {
    return -1;
  }

  @Override
  public long getCurrentRowEndOffset() {
    return -1;
  }

  @Override
  public boolean hasOffsets() {
    return false;
  }
}