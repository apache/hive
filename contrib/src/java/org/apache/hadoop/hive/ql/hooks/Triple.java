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
 * A generic class for triples.
 * @param <T1>
 * @param <T2>
 * @param <T3>
 */
public class Triple<T1, T2, T3>
{
  protected T1 first = null;
  protected T2 second = null;
  protected T3 third = null;

  /**
   * Default constructor.
   */
  public Triple()
  {
  }

  /**
   * Constructor
   * @param a operand
   * @param b operand
   * @param c operand
   */
  public Triple(T1 a, T2 b, T3 c)
  {
    this.first = a;
    this.second = b;
    this.third = c;
  }

  /**
   * Replace the first element of the triple.
   * @param a operand
   */
  public void setFirst(T1 a)
  {
    this.first = a;
  }

  /**
   * Replace the second element of the triple.
   * @param b operand
   */
  public void setSecond(T2 b)
  {
    this.second = b;
  }

  /**
   * Replace the third element of the triple.
   * @param c operand
   */
  public void setThird(T3 c)
  {
    this.third = c;
  }

  /**
   * Return the first element stored in the triple.
   * @return T1
   */
  public T1 getFirst()
  {
    return first;
  }

  /**
   * Return the second element stored in the triple.
   * @return T2
   */
  public T2 getSecond()
  {
    return second;
  }

  /**
   * Return the third element stored in the triple.
   * @return T3
   */
  public T3 getThird()
  {
    return third;
  }

  private boolean equals(Object x, Object y)
  {
    return (x == null && y == null) || (x != null && x.equals(y));
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean equals(Object other)
  {
    return other instanceof Triple &&
      equals(first, ((Triple)other).first) &&
      equals(second, ((Triple)other).second) &&
      equals(third, ((Triple)other).third);
  }

  @Override
  public String toString()
  {
    return "{" + getFirst() + "," + getSecond() + "," + getThird() + "}";
  }
}

