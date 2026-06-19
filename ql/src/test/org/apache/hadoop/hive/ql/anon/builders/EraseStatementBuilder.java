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

package org.apache.hadoop.hive.ql.anon.builders;

import java.util.ArrayList;
import java.util.List;

public class EraseStatementBuilder {

  private String tableName;
  private final List<String> pidList = new ArrayList<>();

  public EraseStatementBuilder() {
  }

  public EraseStatementBuilder(final String tableName) {
    this.tableName = tableName;
  }

  public EraseStatementBuilder withIds(final int... pid) {
    for (int p : pid) {
      pidList.add(Integer.toString(p));
    }
    return this;
  }

  public String build() {
    final String s = String.join(", ", pidList);
    return "ERASE FROM TABLE " + tableName + " FOR IDENTITY VALUES (" + s + ")";
  }

}
