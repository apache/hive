/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore.conf;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

public class StringSetValidator implements Validator {

  private final boolean caseSensitive;
  private final Set<String> expected = new LinkedHashSet<String>();

  public StringSetValidator(String... values) {
    this(false, values);
  }

  public StringSetValidator(boolean caseSensitive, String... values) {
    this.caseSensitive = caseSensitive;
    for (String value : values) {
      expected.add(caseSensitive ? value : value.toLowerCase());
    }
  }

  public Set<String> getExpected() {
    return new HashSet<String>(expected);
  }

  @Override
  public void validate(String value) {
    if (value == null || !expected.contains(caseSensitive ? value : value.toLowerCase())) {
      throw new IllegalArgumentException("Invalid value.. expects one of " + expected);
    }
  }

}
