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

package org.apache.hadoop.hive.ql.hooks;

/**
 * A generic class for pairs.
 * @param <T1>
 * @param <T2>
 */
public class Pair<T1, T2>
{
  protected T1 first = null;
  protected T2 second = null;

  /**
   * Default constructor.
   */
  public Pair()
  {
  }

  /**
   * Constructor
   * @param a operand
   * @param b operand
   */
  public Pair(T1 a, T2 b)
  {
    this.first = a;
    this.second = b;
  }

  /**
   * Replace the first element of the pair.
   * @param a operand
   */
  public void setFirst(T1 a)
  {
    this.first = a;
  }

  /**
   * Replace the second element of the pair.
   * @param b operand
   */
  public void setSecond(T2 b)
  {
    this.second = b;
  }

  /**
   * Return the first element stored in the pair.
   * @return T1
   */
  public T1 getFirst()
  {
    return first;
  }

  /**
   * Return the second element stored in the pair.
   * @return T2
   */
  public T2 getSecond()
  {
    return second;
  }

  private boolean equals(Object x, Object y)
  {
    return (x == null && y == null) || (x != null && x.equals(y));
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean equals(Object other)
  {
    return other instanceof Pair && equals(first, ((Pair)other).first) &&
      equals(second, ((Pair)other).second);
  }

  @Override
  public String toString()
  {
    return "{" + getFirst() + "," + getSecond() + "}";
  }
}

