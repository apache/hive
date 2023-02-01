/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.conf;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_EXPLAIN_NODE_VISIT_LIMIT;
import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link ConfVars#validate} method.
 */
@RunWith(Parameterized.class)
public class TestHiveConfVarsValidate {

  private final ConfVars property;
  private final String value;
  private final String expectedMessage;

  public TestHiveConfVarsValidate(ConfVars property, final String value, final String expectedMessage) {
    this.property = property;
    this.value = value;
    this.expectedMessage = expectedMessage;
  }

  @Parameterized.Parameters(name = "{0} {1}")
  public static Collection<Object[]> generateParameters() {
    List<Object[]> list = new ArrayList<>();
    list.add(new Object[] { HIVE_EXPLAIN_NODE_VISIT_LIMIT, String.valueOf(Integer.MIN_VALUE),
        "Invalid value  -2147483648, which should be in between 1 and 2147483647" });
    list.add(new Object[] { HIVE_EXPLAIN_NODE_VISIT_LIMIT, "-10",
        "Invalid value  -10, which should be in between 1 and 2147483647" });
    list.add(new Object[] { HIVE_EXPLAIN_NODE_VISIT_LIMIT, "0",
        "Invalid value  0, which should be in between 1 and 2147483647" });
    list.add(new Object[] { HIVE_EXPLAIN_NODE_VISIT_LIMIT, "1", null });
    list.add(new Object[] { HIVE_EXPLAIN_NODE_VISIT_LIMIT, "14", null });
    list.add(new Object[] { HIVE_EXPLAIN_NODE_VISIT_LIMIT, String.valueOf(Integer.MAX_VALUE), null });
    return list;
  }

  @Test
  public void testValidate() {
    assertEquals(expectedMessage, property.validate(value));
  }
}
