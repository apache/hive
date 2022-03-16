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
package org.apache.hadoop.hive.ql.parse.type;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.hadoop.hive.ql.parse.RowResolver;

import java.util.Map;

public class HiveRelDataType {
  private final RelDataType relDataType;
  private final RowResolver rowResolver;
  private final Map<String, Integer> hiveNameToPosMap;

  public HiveRelDataType(RelDataType relDataType, RowResolver rowResolver, Map<String, Integer> hiveNameToPosMap) {
    this.relDataType = relDataType;
    this.hiveNameToPosMap = hiveNameToPosMap;
    this.rowResolver = rowResolver;
  }

  public RelDataType getRelDataType() {
    return relDataType;
  }

  public RowResolver getRowResolver() {
    return rowResolver;
  }

  public Map<String, Integer> getHiveNameToPosMap() {
    return hiveNameToPosMap;
  }
}
