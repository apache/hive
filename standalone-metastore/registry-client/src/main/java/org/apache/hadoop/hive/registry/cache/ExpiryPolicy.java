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
package org.apache.hadoop.hive.registry.cache;

import com.fasterxml.jackson.annotation.JsonCreator;

import java.util.concurrent.TimeUnit;

public class ExpiryPolicy {
  private Ttl ttl;
  private long entries;
  private Size size;

  public class Ttl {
    private long count;
    private TimeUnit unit;
    private final long ttlSeconds;

    public Ttl(long count, TimeUnit unit) {
      this.count = count;
      this.unit = unit;
      this.ttlSeconds= unit.toSeconds(count);
    }

    public long getCount() {
      return count;
    }

    public void setCount(long count) {
      this.count = count;
    }

    public TimeUnit getUnit() {
      return unit;
    }

    public void setUnit(TimeUnit unit) {
      this.unit = unit;
    }

    public long getTtlSeconds() {
      return ttlSeconds;
    }
  }

  public class Size {
    private final long count;
    private final long bytes;
    private final BytesUnit unit;

    @JsonCreator
    public Size(long count, BytesUnit unit) {
      this.count = count;
      this.unit = unit;
      this.bytes = unit.toBytes(count);
    }

    public long getCount() {
      return count;
    }

    public long getBytes() {
      return bytes;
    }

    public BytesUnit getUnit() {
      return unit;
    }
  }

  public ExpiryPolicy(Ttl ttl, long entries, Size size) {
    this.ttl = ttl;
    this.entries = entries;
    this.size = size;
  }

  public Ttl getTtl() {
    return ttl;
  }

  public void setTtl(Ttl ttl) {
    this.ttl = ttl;
  }

  public long getEntries() {
    return entries;
  }

  public void setEntries(long entries) {
    this.entries = entries;
  }

  public Size getSize() {
    return size;
  }

  public void setSize(Size size) {
    this.size = size;
  }

  public boolean isTtl() {
    return ttl != null;
  }

  public boolean isEntries() {
    return entries != 0;
  }

  public boolean isSize() {
    return size != null;
  }
}

