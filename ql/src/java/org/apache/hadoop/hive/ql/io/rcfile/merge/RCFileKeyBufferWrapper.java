/**
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

package org.apache.hadoop.hive.ql.io.rcfile.merge;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.RCFile.KeyBuffer;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.compress.CompressionCodec;

public class RCFileKeyBufferWrapper implements
    WritableComparable<RCFileKeyBufferWrapper> {

  protected KeyBuffer keyBuffer;
  protected int recordLength;
  protected int keyLength;
  protected int compressedKeyLength;
  protected Path inputPath;

  protected CompressionCodec codec;

  public RCFileKeyBufferWrapper() {
  }

  public static RCFileKeyBufferWrapper create(KeyBuffer currentKeyBufferObj) {
    RCFileKeyBufferWrapper obj = new RCFileKeyBufferWrapper();
    obj.keyBuffer = currentKeyBufferObj;
    return obj;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    throw new RuntimeException("Not supported.");
  }

  @Override
  public void write(DataOutput out) throws IOException {
    throw new RuntimeException("Not supported.");
  }

  @Override
  public int compareTo(RCFileKeyBufferWrapper o) {
    return this.keyBuffer.compareTo(o.keyBuffer);
  }

  public KeyBuffer getKeyBuffer() {
    return keyBuffer;
  }

  public void setKeyBuffer(KeyBuffer keyBuffer) {
    this.keyBuffer = keyBuffer;
  }

  public int getRecordLength() {
    return recordLength;
  }

  public void setRecordLength(int recordLength) {
    this.recordLength = recordLength;
  }

  public int getKeyLength() {
    return keyLength;
  }

  public void setKeyLength(int keyLength) {
    this.keyLength = keyLength;
  }

  public int getCompressedKeyLength() {
    return compressedKeyLength;
  }

  public void setCompressedKeyLength(int compressedKeyLength) {
    this.compressedKeyLength = compressedKeyLength;
  }

  public Path getInputPath() {
    return inputPath;
  }

  public void setInputPath(Path inputPath) {
    this.inputPath = inputPath;
  }

  public CompressionCodec getCodec() {
    return codec;
  }

  public void setCodec(CompressionCodec codec) {
    this.codec = codec;
  }

}
