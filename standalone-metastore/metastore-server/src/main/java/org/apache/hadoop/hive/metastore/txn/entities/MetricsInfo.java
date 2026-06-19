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
package org.apache.hadoop.hive.metastore.txn.entities;

import java.util.Set;

/**
 * ACID metrics info object.
 */
public class MetricsInfo {

  private int txnToWriteIdCount;
  private int completedTxnsCount;
  private int openReplTxnsCount;
  private int oldestOpenReplTxnId;
  private int oldestOpenReplTxnAge;
  private int openNonReplTxnsCount;
  private int oldestOpenNonReplTxnId;
  private int oldestOpenNonReplTxnAge;
  private int abortedTxnsCount;
  private int oldestAbortedTxnId;
  private int oldestAbortedTxnAge;
  private int locksCount;
  private int oldestLockAge;
  private int tablesWithXAbortedTxnsCount;
  private Set<String> tablesWithXAbortedTxns;
  private int oldestReadyForCleaningAge;

  public int getTxnToWriteIdCount() {
    return txnToWriteIdCount;
  }

  public void setTxnToWriteIdCount(int txnToWriteIdCount) {
    this.txnToWriteIdCount = txnToWriteIdCount;
  }

  public int getCompletedTxnsCount() {
    return completedTxnsCount;
  }

  public void setCompletedTxnsCount(int completedTxnsCount) {
    this.completedTxnsCount = completedTxnsCount;
  }

  public int getOpenReplTxnsCount() {
    return openReplTxnsCount;
  }

  public void setOpenReplTxnsCount(int openReplTxnsCount) {
    this.openReplTxnsCount = openReplTxnsCount;
  }

  public int getOldestOpenReplTxnId() {
    return oldestOpenReplTxnId;
  }

  public void setOldestOpenReplTxnId(int oldestOpenReplTxnId) {
    this.oldestOpenReplTxnId = oldestOpenReplTxnId;
  }

  public int getOldestOpenReplTxnAge() {
    return oldestOpenReplTxnAge;
  }

  public void setOldestOpenReplTxnAge(int oldestOpenReplTxnAge) {
    this.oldestOpenReplTxnAge = oldestOpenReplTxnAge;
  }

  public int getOpenNonReplTxnsCount() {
    return openNonReplTxnsCount;
  }

  public void setOpenNonReplTxnsCount(int openNonReplTxnsCount) {
    this.openNonReplTxnsCount = openNonReplTxnsCount;
  }

  public int getOldestOpenNonReplTxnId() {
    return oldestOpenNonReplTxnId;
  }

  public void setOldestOpenNonReplTxnId(int oldestOpenNonReplTxnId) {
    this.oldestOpenNonReplTxnId = oldestOpenNonReplTxnId;
  }

  public int getOldestOpenNonReplTxnAge() {
    return oldestOpenNonReplTxnAge;
  }

  public void setOldestOpenNonReplTxnAge(int oldestOpenNonReplTxnAge) {
    this.oldestOpenNonReplTxnAge = oldestOpenNonReplTxnAge;
  }

  public int getAbortedTxnsCount() {
    return abortedTxnsCount;
  }

  public void setAbortedTxnsCount(int abortedTxnsCount) {
    this.abortedTxnsCount = abortedTxnsCount;
  }

  public int getOldestAbortedTxnId() {
    return oldestAbortedTxnId;
  }

  public void setOldestAbortedTxnId(int oldestAbortedTxn) {
    this.oldestAbortedTxnId = oldestAbortedTxn;
  }

  public int getOldestAbortedTxnAge() {
    return oldestAbortedTxnAge;
  }

  public void setOldestAbortedTxnAge(int oldestAbortedTxnAge) {
    this.oldestAbortedTxnAge = oldestAbortedTxnAge;
  }

  public void setLocksCount(int locksCount) {
    this.locksCount = locksCount;
  }

  public int getLocksCount() {
    return locksCount;
  }

  public void setOldestLockAge(int oldestLockAge) {
    this.oldestLockAge = oldestLockAge;
  }

  public int getOldestLockAge() {
    return oldestLockAge;
  }

  public int getTablesWithXAbortedTxnsCount() {
    return tablesWithXAbortedTxnsCount;
  }

  public void setTablesWithXAbortedTxnsCount(int tablesWithXAbortedTxnsCount) {
    this.tablesWithXAbortedTxnsCount = tablesWithXAbortedTxnsCount;
  }

  public Set<String> getTablesWithXAbortedTxns() {
    return tablesWithXAbortedTxns;
  }

  public void setTablesWithXAbortedTxns(Set<String> tablesWithXAbortedTxns) {
    this.tablesWithXAbortedTxns = tablesWithXAbortedTxns;
  }

  public int getOldestReadyForCleaningAge() {
    return oldestReadyForCleaningAge;
  }

  public void setOldestReadyForCleaningAge(int oldestReadyForCleaningAge) {
    this.oldestReadyForCleaningAge = oldestReadyForCleaningAge;
  }
}
