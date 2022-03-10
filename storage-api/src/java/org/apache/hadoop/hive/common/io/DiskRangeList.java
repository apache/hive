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
package org.apache.hadoop.hive.common.io;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Java linked list iterator interface is convoluted, and moreover concurrent modifications
 * of the same list by multiple iterators are impossible. Hence, this.
 * Java also doesn't support multiple inheritance, so this cannot be done as "aspect"... */
public class DiskRangeList extends DiskRange {
  private static final Logger LOG = LoggerFactory.getLogger(DiskRangeList.class);
  public DiskRangeList prev, next;

  public DiskRangeList(long offset, long end) {
    super(offset, end);
  }

  /** Replaces this element with another in the list; returns the new element.
   * @param other the disk range to swap into this list
   * @return the new element
   */
  public DiskRangeList replaceSelfWith(DiskRangeList other) {
    checkArg(other);
    other.prev = this.prev;
    other.next = this.next;
    if (this.prev != null) {
      checkOrder(this.prev, other, this);
      this.prev.next = other;
    }
    if (this.next != null) {
      checkOrder(other, this.next, this);
      this.next.prev = other;
    }
    this.next = this.prev = null;
    return other;
  }

  private final static void checkOrder(DiskRangeList prev, DiskRangeList next, DiskRangeList ref) {
    if (prev.getEnd() <= next.getOffset()) return;
    assertInvalidOrder(ref.prev == null ? ref : ref.prev, prev, next);
  }

  private final static void assertInvalidOrder(
      DiskRangeList ref, DiskRangeList prev, DiskRangeList next) {
    String error = "Elements not in order " + prev + " and " + next
        + "; trying to insert into " + stringifyDiskRanges(ref);
    LOG.error(error);
    throw new AssertionError(error);
  }

  // TODO: this duplicates a method in ORC, but the method should actually be here.
  public final static String stringifyDiskRanges(DiskRangeList range) {
    StringBuilder buffer = new StringBuilder();
    buffer.append("[");
    boolean isFirst = true;
    while (range != null) {
      if (!isFirst) {
        buffer.append(", {");
      } else {
        buffer.append("{");
      }
      isFirst = false;
      buffer.append(range.toString());
      buffer.append("}");
      range = range.next;
    }
    buffer.append("]");
    return buffer.toString();
  }

  private void checkArg(DiskRangeList other) {
    if (other == this) {
      // The only case where duplicate elements matter... the others are handled by the below.
      throw new AssertionError("Inserting self into the list [" + other + "]");
    }
    if (other.prev != null || other.next != null) {
      throw new AssertionError("[" + other + "] is part of another list; prev ["
          + other.prev + "], next [" + other.next + "]");
    }
  }

  /**
   * Inserts an intersecting range before current in the list and adjusts offset accordingly.
   * @param other the element to insert
   * @return the new element.
   */
  public DiskRangeList insertPartBefore(DiskRangeList other) {
    checkArg(other);
    if (other.end <= this.offset || other.end > this.end) {
      assertInvalidOrder(this.prev == null ? this : this.prev, other, this);
    }
    this.offset = other.end;
    other.prev = this.prev;
    other.next = this;
    if (this.prev != null) {
      checkOrder(this.prev, other, this.prev);
      this.prev.next = other;
    }
    this.prev = other;
    return other;
  }

  /**
   * Inserts an element after current in the list.
   * @param other the new element to insert
   * @return the new element.
   * */
  public DiskRangeList insertAfter(DiskRangeList other) {
    checkArg(other);
    checkOrder(this, other, this);
    return insertAfterInternal(other);
  }

  private DiskRangeList insertAfterInternal(DiskRangeList other) {
    other.next = this.next;
    other.prev = this;
    if (this.next != null) {
      checkOrder(other, this.next, this);
      this.next.prev = other;
    }
    this.next = other;
    return other;
  }

  /**
   * Inserts an intersecting range after current in the list and adjusts offset accordingly.
   * @param other the new element to insert
   * @return the new element.
   */
  public DiskRangeList insertPartAfter(DiskRangeList other) {
    // The only allowed non-overlapping option is extra bytes at the end.
    if (other.offset > this.end || other.offset <= this.offset || other.end <= this.offset) {
      assertInvalidOrder(this.prev == null ? this : this.prev, this, other);
    }
    this.end = other.offset;
    return insertAfter(other);
  }

  /** Removes an element after current from the list. */
  public void removeAfter() {
    DiskRangeList other = this.next;
    if (this == other) {
      throw new AssertionError("Invalid duplicate [" + other + "]");
    }
    this.next = other.next;
    if (this.next != null) {
      this.next.prev = this;
    }
    other.next = other.prev = null;
  }

  /** Removes the current element from the list. */
  public void removeSelf() {
    if (this.prev == this || this.next == this) {
      throw new AssertionError("Invalid duplicate [" + this + "]");
    }
    if (this.prev != null) {
      this.prev.next = this.next;
    }
    if (this.next != null) {
      this.next.prev = this.prev;
    }
    this.next = this.prev = null;
  }

  /** Splits current element in the list, using DiskRange::slice.
   * @param cOffset the position to split the list
   * @return the split list
   */
  public final DiskRangeList split(long cOffset) {
    DiskRangeList right = insertAfterInternal((DiskRangeList)this.sliceAndShift(cOffset, end, 0));
    DiskRangeList left = replaceSelfWith((DiskRangeList)this.sliceAndShift(offset, cOffset, 0));
    checkOrder(left, right, left); // Prev/next are already checked in the calls.
    return left;
  }

  public boolean hasContiguousNext() {
    return next != null && end == next.offset;
  }

  // @VisibleForTesting
  public int listSize() {
    int result = 1;
    DiskRangeList current = this.next;
    while (current != null) {
      ++result;
      current = current.next;
    }
    return result;
  }

  public long getTotalLength() {
    long totalLength = getLength();
    DiskRangeList current = next;
    while (current != null) {
      totalLength += current.getLength();
      current = current.next;
    }
    return totalLength;
  }

  // @VisibleForTesting
  public DiskRangeList[] listToArray() {
    DiskRangeList[] result = new DiskRangeList[listSize()];
    int i = 0;
    DiskRangeList current = this.next;
    while (current != null) {
      result[i] = current;
      ++i;
      current = current.next;
    }
    return result;
  }

  /**
   * This class provides just a simplistic iterator interface (check {@link DiskRangeList}).
   * Thus, for equality/hashcode just check the actual DiskRange content.
   * @return hashcode
   */
  @Override
  public int hashCode() {
    return super.hashCode();
  }

  @Override
  public boolean equals(Object other) {
    return super.equals(other);
  }

  public static class CreateHelper {
    private DiskRangeList tail = null, head;

    public DiskRangeList getTail() {
      return tail;
    }

    public void addOrMerge(long offset, long end, boolean doMerge, boolean doLogNew) {
      if (doMerge && tail != null && tail.merge(offset, end)) return;
      if (doLogNew) {
        LOG.debug("Creating new range; last range (which can include some previous adds) was "
            + tail);
      }
      DiskRangeList node = new DiskRangeList(offset, end);
      if (tail == null) {
        head = tail = node;
      } else {
        tail = tail.insertAfter(node);
      }
    }

    public DiskRangeList get() {
      return head;
    }

    public DiskRangeList extract() {
      DiskRangeList result = head;
      head = null;
      return result;
    }
  }

  /**
   * List in-place mutation helper - a bogus first element that is inserted before list head,
   * and thus remains constant even if head is replaced with some new range via in-place list
   * mutation. extract() can be used to obtain the modified list.
   */
  public static class MutateHelper extends DiskRangeList {
    public MutateHelper(DiskRangeList head) {
      super(-1, -1);
      assert head != null;
      assert head.prev == null;
      this.next = head;
      head.prev = this;
    }

    public DiskRangeList get() {
      return next;
    }

    public DiskRangeList extract() {
      DiskRangeList result = this.next;
      assert result != null;
      this.next = result.prev = null;
      return result;
    }
  }

  public void setEnd(long newEnd) {
    assert newEnd >= this.offset;
    assert this.next == null || this.next.offset >= newEnd;
    this.end = newEnd;
    if (this.next != null) {
      checkOrder(this, this.next, this);
    }
  }
}
