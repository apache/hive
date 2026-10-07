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
package org.apache.hadoop.hive.metastore.utils;

import org.junit.Assert;
import org.junit.Test;

public class TestCompactionOrderByValidator {

  @Test
  public void testNullAndBlankAccepted() {
    CompactionOrderByValidator.validate(null);
    CompactionOrderByValidator.validate("");
    CompactionOrderByValidator.validate("   ");
  }

  @Test
  public void testPlainOrderByAccepted() {
    CompactionOrderByValidator.validate("order by col1");
    CompactionOrderByValidator.validate("ORDER BY col1 ASC");
    CompactionOrderByValidator.validate("order by col1 desc, col2");
    CompactionOrderByValidator.validate("Order By col1 asc nulls first, col2 DESC NULLS LAST");
    CompactionOrderByValidator.validate("order by `quoted col`, t.col2");
    CompactionOrderByValidator.validate("order by 1, 2 desc");
  }

  @Test
  public void testInjectionsRejected() {
    // statement tail smuggling (privileged data copy)
    assertRejected("order by 1 union all select * from finance.salaries");
    // empty-rewrite data destruction
    assertRejected("order by 1 limit 0");
    // expressions / function calls
    assertRejected("order by upper(col1)");
    assertRejected("order by (select 1)");
    // comment smuggling and statement separators
    assertRejected("order by col1 -- comment");
    assertRejected("order by col1; drop table t");
    // missing ORDER BY prefix
    assertRejected("col1 asc");
    assertRejected("union all select 1");
  }

  private static void assertRejected(String clause) {
    Assert.assertThrows(IllegalArgumentException.class, () -> CompactionOrderByValidator.validate(clause));
  }
}
