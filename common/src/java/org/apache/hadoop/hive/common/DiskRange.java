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
package org.apache.hadoop.hive.common;

import java.nio.ByteBuffer;

/**
 * The sections of a file.
 */
public class DiskRange {
  /** The first address. */
  protected long offset;
  /** The address afterwards. */
  protected long end;

  public DiskRange(long offset, long end) {
    this.offset = offset;
    this.end = end;
    if (end < offset) {
      throw new IllegalArgumentException("invalid range " + this);
    }
  }

  @Override
  public boolean equals(Object other) {
    if (other == null || other.getClass() != getClass()) {
      return false;
    }
    DiskRange otherR = (DiskRange) other;
    return otherR.offset == offset && otherR.end == end;
  }

  @Override
  public int hashCode() {
    return (int)(offset ^ (offset >>> 32)) * 31 + (int)(end ^ (end >>> 32));
  }

  @Override
  public String toString() {
    return "range start: " + offset + " end: " + end;
  }

  public long getOffset() {
    return offset;
  }

  public long getEnd() {
    return end;
  }

  public int getLength() {
    long len = this.end - this.offset;
    assert len <= Integer.MAX_VALUE;
    return (int)len;
  }

  // For subclasses
  public boolean hasData() {
    return false;
  }

  public DiskRange sliceAndShift(long offset, long end, long shiftBy) {
    // Rather, unexpected usage exception.
    throw new UnsupportedOperationException();
  }

  public ByteBuffer getData() {
    throw new UnsupportedOperationException();
  }

  protected boolean merge(long otherOffset, long otherEnd) {
    if (!overlap(offset, end, otherOffset, otherEnd)) return false;
    offset = Math.min(offset, otherOffset);
    end = Math.max(end, otherEnd);
    return true;
  }

  private static boolean overlap(long leftA, long rightA, long leftB, long rightB) {
    if (leftA <= leftB) {
      return rightA >= leftB;
    }
    return rightB >= leftA;
  }
}