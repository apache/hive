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

package org.apache.hadoop.hive.ql.io.orc;

/**
 * An enumeration that lists the generic compression algorithms that
 * can be applied to ORC files. This is a shim to help users while we
 * migrate to the org.apache.orc package.
 */
public enum CompressionKind {
  NONE(org.apache.orc.CompressionKind.NONE),
  ZLIB(org.apache.orc.CompressionKind.ZLIB),
  SNAPPY(org.apache.orc.CompressionKind.SNAPPY),
  LZO(org.apache.orc.CompressionKind.LZO);

  CompressionKind(org.apache.orc.CompressionKind underlying) {
    this.underlying = underlying;
  }

  public org.apache.orc.CompressionKind getUnderlying() {
    return underlying;
  }

  private final org.apache.orc.CompressionKind underlying;
}
