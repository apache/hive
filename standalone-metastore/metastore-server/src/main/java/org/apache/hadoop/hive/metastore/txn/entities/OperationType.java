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
package org.apache.hadoop.hive.metastore.txn.entities;

import org.apache.hadoop.hive.metastore.api.DataOperationType;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

/**
 * These are the valid values for TXN_COMPONENTS.TC_OPERATION_TYPE.
 */
public enum OperationType {
  SELECT('s', DataOperationType.SELECT),
  INSERT('i', DataOperationType.INSERT),
  UPDATE('u', DataOperationType.UPDATE),
  DELETE('d', DataOperationType.DELETE),
  COMPACT('c', null);

  private final char sqlConst;
  private final DataOperationType dataOperationType;

  private static final Map<DataOperationType, OperationType> DOT_LOOKUP =
      Arrays.stream(OperationType.values()).collect(toMap(OperationType::getDataOperationType, identity()));

  OperationType(char sqlConst, DataOperationType dataOperationType) {
    this.sqlConst = sqlConst;
    this.dataOperationType = dataOperationType;
  }

  public String toString() {
    return "'" + getSqlConst() + "'";
  }

  public static OperationType fromDataOperationType(DataOperationType dop) {
    return Optional.of(DOT_LOOKUP.get(dop)).orElseThrow(IllegalArgumentException::new);
  }

  public String getSqlConst() {
    return Character.toString(sqlConst);
  }

  public DataOperationType getDataOperationType() {
    return dataOperationType;
  }
}
