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
package org.apache.hadoop.hive.ql.optimizer.calcite;

/**
 * Holder for a list of constants describing which bugs have not been
 * fixed.
 *
 * <p>The usage of the constant is a convenient way to identify the impact of
 * the bug. When someone fixes the bug, they will remove the constant and all
 * usages of it. Also, the constant helps track the propagation of the fix: as
 * the fix is integrated into other branches, the constant will be removed from
 * those branches.</p>
 *
 */
public final class Bug {

  /**
   * Whether <a href="https://issues.apache.org/jira/browse/CALCITE-1851">CALCITE-1851</a> is fixed.
   */
  public static final boolean CALCITE_1851_FIXED = false;
  
  /**
   * Whether <a href="https://issues.apache.org/jira/browse/CALCITE-3982">issue
   * CALCITE-3982</a> is fixed.
   */
  public static final boolean CALCITE_3982_FIXED = false;

  /**
   * Whether <a href="https://issues.apache.org/jira/browse/CALCITE-4166">issue
   * CALCITE-4166</a> is fixed.
   */
  public static final boolean CALCITE_4166_FIXED = false;

  /**
   * Whether <a href="https://issues.apache.org/jira/browse/CALCITE-4499">issue
   * CALCITE-4499</a> is fixed.
   */
  public static final boolean CALCITE_4499_FIXED = false;

  /**
   * Whether <a href="https://issues.apache.org/jira/browse/CALCITE-4574">CALCITE-4574</a> is fixed.
   */
  public static final boolean CALCITE_4574_FIXED = false;
}
