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
package org.apache.hadoop.hive.ql.parse;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class TestNonReservedWords {
  @Parameters(name = "{0}")
  public static Collection<String> data() {
    Set<String> keywords = new HashSet<>(HiveParser.getKeywords());
    Arrays
        .stream(HiveParser.tokenNames)
        .filter(token -> token.startsWith("KW_"))
        .map(token -> token.replaceFirst("^KW_", ""))
        .forEach(keywords::add);
    Set<String> reservedWords = new HashSet<>(TestReservedWords.data());
    return keywords
        .stream()
        .filter(keyword -> keyword.matches("[a-zA-Z0-9_]+"))
        .filter(keyword -> !reservedWords.contains(keyword))
        .collect(Collectors.toList());
  }

  private static final Configuration conf = new Configuration();
  private static final ParseDriver pd = new ParseDriver();

  private final String keyword;

  public TestNonReservedWords(String keyword) {
    this.keyword = keyword;
  }

  @Test
  public void testNonReservedWords() throws Exception {
    String query = String.format("CREATE TABLE %s (col STRING)", keyword);
    pd.parse(query, conf);
  }
}
