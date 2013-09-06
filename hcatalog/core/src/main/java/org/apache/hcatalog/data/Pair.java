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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hcatalog.data;

import java.io.Serializable;

/**
 * Copy of C++ STL pair container.
 * @deprecated Use/modify {@link org.apache.hive.hcatalog.data.Pair} instead
 */
public class Pair<T, U> implements Serializable {

  private static final long serialVersionUID = 1L;
  public T first;
  public U second;

  /**
   * @param f First element in pair.
   * @param s Second element in pair.
   */
  public Pair(T f, U s) {
    first = f;
    second = s;
  }

  /* (non-Javadoc)
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return "[" + first.toString() + "," + second.toString() + "]";
  }

  @Override
  public int hashCode() {
    return (((this.first == null ? 1 : this.first.hashCode()) * 17)
      + (this.second == null ? 1 : this.second.hashCode()) * 19);
  }

  @Override
  public boolean equals(Object other) {
    if (other == null) {
      return false;
    }

    if (!(other instanceof Pair)) {
      return false;
    }

    Pair otherPair = (Pair) other;

    if (this.first == null) {
      if (otherPair.first != null) {
        return false;
      } else {
        return true;
      }
    }

    if (this.second == null) {
      if (otherPair.second != null) {
        return false;
      } else {
        return true;
      }
    }

    if (this.first.equals(otherPair.first) && this.second.equals(otherPair.second)) {
      return true;
    } else {
      return false;
    }
  }
}
