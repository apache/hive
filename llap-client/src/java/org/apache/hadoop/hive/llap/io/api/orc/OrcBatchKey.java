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

public class OrcBatchKey {
  public long file;
  public int stripeIx, rgIx;

  public OrcBatchKey(long file, int stripeIx, int rgIx) {
    this.file = file;
    this.stripeIx = stripeIx;
    this.rgIx = rgIx;
  }

  @Override
  public String toString() {
    return "[" + file + ", stripe " + stripeIx + ", rgIx " + rgIx + "]";
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = prime + (int)(file ^ (file >>> 32));
    return (prime * result + rgIx) * prime + stripeIx;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (!(obj instanceof OrcBatchKey)) return false;
    OrcBatchKey other = (OrcBatchKey)obj;
    // Strings are interned and can thus be compared like this.
    return stripeIx == other.stripeIx && rgIx == other.rgIx && file == other.file;
  }

  @Override
  public OrcBatchKey clone() throws CloneNotSupportedException {
    return new OrcBatchKey(file, stripeIx, rgIx);
  }
}