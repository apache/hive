/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.beeline;

import org.junit.Test;

import static org.apache.hive.common.util.HiveStringUtils.removeComments;
import static org.junit.Assert.assertEquals;

public class TestCommands {

  @Test
  public void testLinesEndingWithComments() {
    int[] escape = {-1};
    assertEquals("show tables;", removeComments("show tables;",escape));
    assertEquals("show tables;", removeComments("show tables; --comments",escape));
    assertEquals("show tables;", removeComments("show tables; -------comments",escape));
    assertEquals("show tables;", removeComments("show tables; -------comments;one;two;three;;;;",escape));
    assertEquals("show", removeComments("show-- tables; -------comments",escape));
    assertEquals("show", removeComments("show --tables; -------comments",escape));
    assertEquals("s", removeComments("s--how --tables; -------comments",escape));
    assertEquals("", removeComments("-- show tables; -------comments",escape));

    assertEquals("\"show tables\"", removeComments("\"show tables\" --comments",escape));
    assertEquals("\"show --comments tables\"", removeComments("\"show --comments tables\" --comments",escape));
    assertEquals("\"'show --comments' tables\"", removeComments("\"'show --comments' tables\" --comments",escape));
    assertEquals("'show --comments tables'", removeComments("'show --comments tables' --comments",escape));
    assertEquals("'\"show --comments tables\"'", removeComments("'\"show --comments tables\"' --comments",escape));
  }
}

