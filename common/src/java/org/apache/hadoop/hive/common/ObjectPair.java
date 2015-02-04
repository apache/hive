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



public class ObjectPair<F, S> {
  private F first;
  private S second;

  public ObjectPair() {}

  /**
   * Creates a pair. Constructor doesn't infer template args but
   * the method does, so the code becomes less ugly.
   */
  public static <T1, T2> ObjectPair<T1, T2> create(T1 f, T2 s) {
    return new ObjectPair<T1, T2>(f, s);
  }

  public ObjectPair(F first, S second) {
    this.first = first;
    this.second = second;
  }

  public F getFirst() {
    return first;
  }

  public void setFirst(F first) {
    this.first = first;
  }

  public S getSecond() {
    return second;
  }

  public void setSecond(S second) {
    this.second = second;
  }

  @Override
  public boolean equals(Object that) {
    if (that == null) {
      return false;
    }
    if (that instanceof ObjectPair) {
      return this.equals((ObjectPair<F, S>)that);
    }
    return false;
  }

  public boolean equals(ObjectPair<F, S> that) {
    if (that == null) {
      return false;
    }

    return this.getFirst().equals(that.getFirst()) &&
        this.getSecond().equals(that.getSecond());
  }

  @Override
  public int hashCode() {
    return first.hashCode() * 31 + second.hashCode();
  }

  public String toString() {
    return first + ":" + second;
  }
}
