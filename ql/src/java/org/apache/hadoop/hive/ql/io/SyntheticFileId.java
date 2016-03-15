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

package org.apache.hadoop.hive.ql.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;

public final class SyntheticFileId implements Writable {
  private long pathHash;
  private long modTime;
  private long length;

  /** Writable ctor. */
  public SyntheticFileId() {
  }

  public SyntheticFileId(Path path, long len, long modificationTime) {
    this.pathHash = hashCode(path.toUri().getPath());
    this.modTime = modificationTime;
    this.length = len;
  }

  public SyntheticFileId(FileStatus file) {
    this(file.getPath(), file.getLen(), file.getModificationTime());
  }

  @Override
  public String toString() {
    return "[" + pathHash + ", " + modTime + ", " + length + "]";
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = prime + (int) (length ^ (length >>> 32));
    result = prime * result + (int) (modTime ^ (modTime >>> 32));
    return prime * result + (int) (pathHash ^ (pathHash >>> 32));
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (!(obj instanceof SyntheticFileId)) return false;
    SyntheticFileId other = (SyntheticFileId)obj;
    return length == other.length && modTime == other.modTime && pathHash == other.pathHash;
  }

  private long hashCode(String path) {
    long h = 0;
    for (int i = 0; i < path.length(); ++i) {
      h = 1223 * h + path.charAt(i);
    }
    return h;
  }

  /** Length allows for some backward compatibility wrt field addition. */
  private static final short THREE_LONGS = 24;

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeShort(THREE_LONGS);
    out.writeLong(pathHash);
    out.writeLong(modTime);
    out.writeLong(length);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    short len = in.readShort();
    if (len < THREE_LONGS) throw new IOException("Need at least " + THREE_LONGS + " bytes");
    pathHash = in.readLong();
    modTime = in.readLong();
    length = in.readLong();
    int extraBytes = len - THREE_LONGS;
    if (extraBytes > 0) {
      in.skipBytes(extraBytes);
    }
  }
}