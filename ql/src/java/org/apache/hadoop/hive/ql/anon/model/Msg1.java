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

package org.apache.hadoop.hive.ql.anon.model;

public class Msg1 extends BaseMsg {
  private long txnId;
  private String value1;
  private String value2;
  private String value3;

  public Msg1() {
  }

  public Msg1(final long txnId, final String value1, final String value2, final String value3) {
    setTxnId(txnId);
    setValue1(value1);
    setValue2(value2);
    setValue3(value3);
  }

  public long getTxnId() {
    return txnId;
  }

  public void setTxnId(final long txnId) {
    this.txnId = txnId;
  }

  public void setValue1(String value1) {
    this.value1 = value1;
  }

  public String getValue1() {
    return value1;
  }

  public void setValue2(String value2) {
    this.value2 = value2;
  }

  public String getValue2() {
    return value2;
  }

  public void setValue3(String value3) {
    this.value3 = value3;
  }

  public String getValue3() {
    return value3;
  }
}
