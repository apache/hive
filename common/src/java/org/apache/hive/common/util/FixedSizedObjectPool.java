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
package org.apache.hive.common.util;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.hive.common.Pool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/** Simple object pool of limited size. Implemented as a lock-free ring buffer;
 * may fail to produce items if there are too many concurrent users. */
public class FixedSizedObjectPool<T> implements Pool<T> {
  public static final Logger LOG = LoggerFactory.getLogger(FixedSizedObjectPool.class);

  /**
   * Ring buffer has two "markers" - where objects are present ('objects' list), and where they are
   * removed ('empty' list). This class contains bit shifts and masks for one marker's components
   * within a long, and provides utility methods to get/set the components.
   * Marker consists of (examples here for 'objects' list; same for 'empty' list):
   *  - the marker itself. Set to NO_MARKER if list is empty (e.g. no objects to take from pool),
   *    otherwise contains the array index of the first element of the list.
   *  - the 'delta'. Number of elements from the marker that is being modified. Each concurrent
   *    modification (e.g. take call) increments this to claim an array index. Delta elements
   *    from the marker cannot be touched by other threads. Delta can never overshoot the other
   *    marker (or own marker if other is empty), or overflow MAX_DELTA. If delta is set to
   *    NO_DELTA, it means the marker has been modified during 'take' operation and list cannot
   *    be touched (see below). In any of these cases, take returns null.
   *  - the 'refcount'/'rc'. Number of operations occurring on the marker. Each e.g. take incs
   *    this; when the last of the overlapping operations decreases the refcount, it 'commits'
   *    the modifications by moving the marker according to delta and resetting delta to 0.
   *    If the other list does not exist, it's also created (i.e. first 'offer' to a new pool with
   *    empty 'objects' list will create the 'objects' list); if the list is being exhausted to empty
   *    by other op (e.g. pool has 2 objects, 2 takes are in progress when offer commits), the
   *    marker of the other list is still reset to new location, and delta is set to NO_DELTA,
   *    preventing operations on the lists until the exhausting ops commit and set delta to 0.
   */
  private static final class Marker {
    // Currently the long must fit 2 markers. Setting these bit sizes determines the balance
    // between max pool size allowed and max concurrency allowed. This balance here is not what we
    // want (up to 254 of each op while only 65535 objects limit), but it uses whole bytes and is
    // good for now. Delta and RC take the same number of bits; usually it doesn't make sense to
    // have more delta.
    private static final long MARKER_MASK = 0xffffL, DELTA_MASK = 0xffL, RC_MASK = 0xffL;
    public Marker(int markerShift, int deltaShift, int rcShift) {
      this.markerShift = markerShift;
      this.deltaShift = deltaShift;
      this.rcShift = rcShift;
    }
    int markerShift, deltaShift, rcShift;

    public final long setMarker(long dest, long val) {
      return setValue(dest, val, markerShift, MARKER_MASK);
    }

    public final long setDelta(long dest, long val) {
      return setValue(dest, val, deltaShift, DELTA_MASK);
    }

    public final long setRc(long dest, long val) {
      return setValue(dest, val, rcShift, RC_MASK);
    }

    public final long getMarker(long src) {
      return getValue(src, markerShift, MARKER_MASK);
    }

    public final long getDelta(long src) {
      return getValue(src, deltaShift, DELTA_MASK);
    }

    public final long getRc(long src) {
      return getValue(src, rcShift, RC_MASK);
    }

    private final long setValue(long dest, long val, int offset, long mask) {
      return (dest & (~(mask << offset))) + (val << offset);
    }

    private final long getValue(long src, int offset, long mask) {
      return (src >>> offset) & mask;
    }

    public String toString(long markers) {
      return "{" + getMarker(markers) + ", " + getDelta(markers) + ", " + getRc(markers) + "}";
    }
  }
  private static final long NO_MARKER = Marker.MARKER_MASK, NO_DELTA = Marker.DELTA_MASK,
      MAX_DELTA = NO_DELTA - 1, MAX_SIZE = NO_MARKER - 1;
  private static final long NO_INDEX = 0; // The array index can't be reserved.

  // See Marker class comment.
  private static final Marker OBJECTS = new Marker(48, 40, 32);
  private static final Marker EMPTY = new Marker(16, 8, 0);
  private final AtomicLong state;
  private final PoolObjectHelper<T> helper;
  private final T[] pool;

  public FixedSizedObjectPool(int size, PoolObjectHelper<T> helper) {
    this(size, helper, LOG.isTraceEnabled());
  }

  @VisibleForTesting
  public FixedSizedObjectPool(int size, PoolObjectHelper<T> helper, boolean doTraceLog) {
    if (size > MAX_SIZE) {
      throw new AssertionError("Size must be <= " + MAX_SIZE);
    }
    this.helper = helper;
    @SuppressWarnings("unchecked")
    T[] poolTmp = (T[])new Object[size];
    pool = poolTmp;
    // Initially, all deltas and rcs are 0; empty list starts at 0; there are no objects to take.
    state = new AtomicLong(OBJECTS.setMarker(0, NO_MARKER));
    casLog = doTraceLog ? new CasLog() : null;
  }

  @Override
  public T take() {
    T result = pool.length > 0 ? takeImpl() : null;
    return (result == null) ? helper.create() : result;
  }

  @Override
  public void offer(T t) {
    tryOffer(t);
  }

  @Override
  public int size() {
    return pool.length;
  }

  @VisibleForTesting
  public boolean tryOffer(T t) {
    if (t == null || pool.length == 0) return false; // 0 size means no-pooling case - passthru.
    helper.resetBeforeOffer(t);
    return offerImpl(t);
  }

  private T takeImpl() {
    long oldState = reserveArrayIndex(OBJECTS, EMPTY);
    if (oldState == NO_INDEX) return null; // For whatever reason, reserve failed.
    long originalMarker = OBJECTS.getMarker(oldState), delta = OBJECTS.getDelta(oldState);
    int arrayIndex = (int)getArrayIndex(originalMarker, delta);
    T result = pool[arrayIndex];
    if (result == null) {
      throwError(oldState, arrayIndex, "null");
    }
    pool[arrayIndex] = null;
    commitArrayIndex(OBJECTS, EMPTY, originalMarker);
    return result;
  }

  private boolean offerImpl(T t) {
    long oldState = reserveArrayIndex(EMPTY, OBJECTS);
    if (oldState == NO_INDEX) return false; // For whatever reason, reserve failed.
    long originalMarker = EMPTY.getMarker(oldState), delta = EMPTY.getDelta(oldState);
    int arrayIndex = (int)getArrayIndex(originalMarker, delta);
    if (pool[arrayIndex] != null) {
      throwError(oldState, arrayIndex, "non-null");
    }
    pool[arrayIndex] = t;
    commitArrayIndex(EMPTY, OBJECTS, originalMarker);
    return true;
  }

  private void throwError(long oldState, int arrayIndex, String type) {
    long newState = state.get();
    if (casLog != null) {
      casLog.dumpLog(true);
    }
    String msg = "Unexpected " + type + " at " + arrayIndex + "; state was "
        + toString(oldState) + ", now " + toString(newState);
    LOG.info(msg);
    throw new AssertionError(msg);
  }

  private long reserveArrayIndex(Marker from, Marker to) {
    while (true) {
      long oldVal = state.get(), marker = from.getMarker(oldVal), delta = from.getDelta(oldVal),
          rc = from.getRc(oldVal), toMarker = to.getMarker(oldVal), toDelta = to.getDelta(oldVal);
      if (marker == NO_MARKER) return NO_INDEX; // The list is empty.
      if (delta == MAX_DELTA) return NO_INDEX; // Too many concurrent operations; spurious failure.
      if (delta == NO_DELTA) return NO_INDEX; // List is drained and recreated concurrently.
      if (toDelta == NO_DELTA) { // Same for the OTHER list; spurious.
        // TODO: the fact that concurrent re-creation of other list necessitates full stop is not
        //       ideal... the reason is that the list NOT being re-created still uses the list
        //       being re-created for boundary check; it needs the old value of the other marker.
        //       However, NO_DELTA means the other marker was already set to a new value. For now,
        //       assume concurrent re-creation is rare and the gap before commit is tiny.
        return NO_INDEX;
      }
      assert rc <= delta; // There can never be more concurrent takers than uncommitted ones.

      long newDelta = incDeltaValue(marker, toMarker, delta); // Increase target list pos.
      if (newDelta == NO_DELTA) return NO_INDEX; // Target list is being drained.
      long newVal = from.setRc(from.setDelta(oldVal, newDelta), rc + 1); // Set delta and refcount.
      if (setState(oldVal, newVal)) return oldVal;
    }
  }

  private void commitArrayIndex(Marker from, Marker to, long originalMarker) {
    while (true) {
      long oldVal = state.get(), rc = from.getRc(oldVal);
      long newVal = from.setRc(oldVal, rc - 1); // Decrease refcount.
      assert rc > 0;
      if (rc == 1) {
        // We are the last of the concurrent operations to finish. Commit.
        long marker = from.getMarker(oldVal), delta = from.getDelta(oldVal),
            otherMarker = to.getMarker(oldVal), otherDelta = to.getDelta(oldVal);
        assert rc <= delta;
        // Move marker according to delta, change delta to 0.
        long newMarker = applyDeltaToMarker(marker, otherMarker, delta);
        newVal = from.setDelta(from.setMarker(newVal, newMarker), 0);
        if (otherMarker == NO_MARKER) {
          // The other list doesn't exist, create it at the first index of our op.
          assert otherDelta == 0;
          newVal = to.setMarker(newVal, originalMarker);
        } else if (otherDelta > 0 && otherDelta != NO_DELTA
            && applyDeltaToMarker(otherMarker, marker, otherDelta) == NO_MARKER) {
          // The other list will be exhausted when it commits. Create new one pending that commit.
          newVal = to.setDelta(to.setMarker(newVal, originalMarker), NO_DELTA);
        }
      }
      if (setState(oldVal, newVal)) return;
    }
  }

  private boolean setState(long oldVal, long newVal) {
    boolean result = state.compareAndSet(oldVal, newVal);
    if (result && casLog != null) {
      casLog.log(oldVal, newVal);
    }
    return result;
  }

  private long incDeltaValue(long markerFrom, long otherMarker, long delta) {
    if (delta == pool.length) return NO_DELTA; // The (pool-sized) list is being fully drained.
    long result = delta + 1;
    if (getArrayIndex(markerFrom, result) == getArrayIndex(otherMarker, 1)) {
      return NO_DELTA; // The list is being drained, cannot increase the delta anymore.
    }
    return result;
  }

  private long applyDeltaToMarker(long marker, long markerLimit, long delta) {
    if (delta == NO_DELTA) return marker; // List was recreated while we were exhausting it.
    if (delta == pool.length) {
      assert markerLimit == NO_MARKER; // If we had the entire pool, other list couldn't exist.
      return NO_MARKER; // We exhausted the entire-pool-sized list.
    }
    marker = getArrayIndex(marker, delta); // Just move the marker according to delta.
    if (marker == markerLimit) return NO_MARKER; // We hit the limit - the list was exhausted.
    return marker;
  }

  private long getArrayIndex(long marker, long delta) {
    marker += delta;
    if (marker >= pool.length) {
      marker -= pool.length; // Wrap around at the end of buffer.
    }
    return marker;
  }

  static String toString(long markers) {
    return OBJECTS.toString(markers) + ", " + EMPTY.toString(markers);
  }

  // TODO: Temporary for debugging. Doesn't interfere with MTT failures (unlike LOG.debug).
  private final static class CasLog {
    private final int size;
    private final long[] log;
    private final AtomicLong offset = new AtomicLong(-1);

    public CasLog() {
      size = 1 << 14 /* 256Kb in longs */;
      log = new long[size];
    }

    public void log(long oldVal, long newVal) {
      int ix = (int)((offset.incrementAndGet() << 1) & (size - 1));
      log[ix] = oldVal;
      log[ix + 1] = newVal;
    }

    public synchronized void dumpLog(boolean doSleep) {
      if (doSleep) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
        }
      }
      int logSize = (int)offset.get();
      // TODO: dump the end if wrapping around?
      for (int i = 0; i < logSize; ++i) {
        LOG.info("CAS history dump: " + FixedSizedObjectPool.toString(log[i << 1]) + " => "
            + FixedSizedObjectPool.toString(log[(i << 1) + 1]));
      }
      offset.set(0);
    }
  }
  private final CasLog casLog;
}
