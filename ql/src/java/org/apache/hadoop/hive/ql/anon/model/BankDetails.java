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

public class BankDetails extends BaseMsg {
  private String bankName;
  private String cardNum;
  private String pinCode;

  public BankDetails() {
  }

  public BankDetails(final String bankName, final String cardNum, final String pinCode) {
    setBankName(bankName);
    setCardNum(cardNum);
    setPinCode(pinCode);
  }

  public void setBankName(String bankName) {
    this.bankName = bankName;
  }

  public String getBankName() {
    return bankName;
  }

  public void setCardNum(String cardNum) {
    this.cardNum = cardNum;
  }

  public String getCardNum() {
    return cardNum;
  }

  public void setPinCode(String pinCode) {
    this.pinCode = pinCode;
  }

  public String getPinCode() {
    return pinCode;
  }
}
