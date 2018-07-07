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

public enum BytesUnit {
  BYTES {
    public long toBytes(long d) { return d; }
    public long toKilobytes(long d) { return d/RATIO; }
    public long toMegabytes(long d)  { return d/RATIO_POW_2; }
  },
  KILOBYTES {
    public long toBytes(long d)  { return d*RATIO; }
    public long toKilobytes(long d) { return d; }
    public long toMegabytes(long d)  { return d/RATIO; }
  },
  MEGABYTES {
    public long toBytes(long d)  { return d*RATIO_POW_2; }
    public long toKilobytes(long d) { return d*RATIO; }
    public long toMegabytes(long d)  { return d; }
  };

  private static final long RATIO = 1024;
  private static final long RATIO_POW_2 = 1024*1024;

  public long toBytes(long d)  {throw new AbstractMethodError(); }
  public long toKilobytes(long d) {throw new AbstractMethodError(); }
  public long toMegabytes(long d) {throw new AbstractMethodError(); }
}
