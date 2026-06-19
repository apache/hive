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

import org.apache.hadoop.hive.metastore.api.TxnState;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

/**
 * These are the valid values for TXNS.TXN_STATE.
 */
public enum TxnStatus {
  OPEN('o', TxnState.OPEN),
  ABORTED('a', TxnState.ABORTED),
  COMMITTED('c', TxnState.COMMITTED),
  UNKNOWN('u', null);

  private final char sqlConst;
  private final TxnState txnState;

  private static final Map<String, TxnStatus> LOOKUP =
      Arrays.stream(TxnStatus.values()).collect(toMap(TxnStatus::getSqlConst, identity()));

  TxnStatus(char sqlConst, TxnState txnState) {

    this.sqlConst = sqlConst;
    this.txnState = txnState;
  }

  public String toString() {
    return "'" + getSqlConst() + "'";
  }

  public String getSqlConst() {
    return Character.toString(sqlConst);
  }

  public TxnState toTxnState() {
    return Optional.of(txnState).orElseThrow(IllegalArgumentException::new);
  }

  public static TxnStatus fromString(String sqlConst) {
    return Optional.of(LOOKUP.get(sqlConst)).orElseThrow(IllegalArgumentException::new);
  }
}
