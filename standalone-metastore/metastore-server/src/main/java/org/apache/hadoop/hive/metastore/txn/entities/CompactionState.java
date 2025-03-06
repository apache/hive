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

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

/**
 * These are the valid values for Compaction states.
 */
public enum CompactionState {
  INITIATED('i', "initiated"),
  WORKING('w', "working"),
  READY_FOR_CLEANING('r', "ready for cleaning"),
  FAILED('f', "failed"),
  SUCCEEDED('s', "succeeded"),
  DID_NOT_INITIATE('a', "did not initiate"),
  REFUSED('c', "refused"),
  ABORTED('x', "aborted");

  private final String message;
  private final char sqlConst;

  private static final Map<String, CompactionState> LOOKUP =
      Arrays.stream(CompactionState.values()).collect(toMap(CompactionState::getSqlConst, identity()));

  CompactionState(char sqlConst, String message) {
    this.sqlConst = sqlConst;
    this.message = message;
  }

  @Override
  public String toString() {
    return name().toLowerCase().replace("_", " ");
  }

  public String getSqlConst() {
    return Character.toString(sqlConst);
  }

  public static CompactionState fromSqlConst(char sqlConst) {
    return fromSqlConst(sqlConst + "");
  }
  
  public static CompactionState fromSqlConst(String sqlConst) {
    return Optional.of(LOOKUP.get(sqlConst)).orElseThrow(IllegalArgumentException::new);
  }

  public static CompactionState fromString(String inputValue) {
    return CompactionState.valueOf(inputValue.toUpperCase().replace(" ", "_"));
  }
}