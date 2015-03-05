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

package org.apache.hadoop.hive.llap.io.api.orc;

public class OrcCacheKey extends OrcBatchKey {
  public int colIx;

  public OrcCacheKey(long file, int stripeIx, int rgIx, int colIx) {
    super(file, stripeIx, rgIx);
    this.colIx = colIx;
  }

  public OrcCacheKey(OrcBatchKey batchKey, int colIx) {
    super(batchKey.file, batchKey.stripeIx, batchKey.rgIx);
    this.colIx = colIx;
  }

  public OrcBatchKey copyToPureBatchKey() {
    return new OrcBatchKey(file, stripeIx, rgIx);
  }

  @Override
  public String toString() {
    return "[" + file + ", stripe " + stripeIx + ", rgIx " + rgIx + ", rgIx " + colIx + "]";
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    return super.hashCode() * prime + colIx;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (!(obj instanceof OrcCacheKey)) return false;
    OrcCacheKey other = (OrcCacheKey)obj;
    // Strings are interned and can thus be compared like this.
    return stripeIx == other.stripeIx && rgIx == other.rgIx
        && file == other.file && other.colIx == colIx;
  }
}