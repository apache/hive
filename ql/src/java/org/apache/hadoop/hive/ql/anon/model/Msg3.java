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

import java.util.List;

public class Msg3 extends BaseMsg {
  private int userId;
  private String firstName;
  private String lastName;
  private String address;
  private String city;
  private String country;
  private String email;
  private String telephone;
  private String birthDate;
  private String value;
  private List<String> ipList;
  private BankDetails bankDetails;

  public Msg3() {
  }

  public Msg3(final int userId, final String firstName, final String lastName, final String address, final String city, final String country,
              final String email, final String telephone, final String birthDate, final String value, final List<String> ipList, final BankDetails bankDetails) {
    setUserId(userId);
    setFirstName(firstName);
    setLastName(lastName);
    setAddress(address);
    setCity(city);
    setCountry(country);
    setEmail(email);
    setTelephone(telephone);
    setBirthDate(birthDate);
    setValue(value);
    setIpList(ipList);
    setBankDetails(bankDetails);
  }

  public int getUserId() {
    return userId;
  }

  public void setUserId(final int userId) {
    this.userId = userId;
  }

  public String getAddress() {
    return address;
  }

  public void setAddress(final String address) {
    this.address = address;
  }

  public String getTelephone() {
    return telephone;
  }

  public void setTelephone(final String telephone) {
    this.telephone = telephone;
  }

  public String getCountry() {
    return country;
  }

  public void setCountry(final String country) {
    this.country = country;
  }

  public String getCity() {
    return city;
  }

  public void setCity(final String city) {
    this.city = city;
  }

  public String getEmail() {
    return email;
  }

  public void setEmail(final String email) {
    this.email = email;
  }

  public String getFirstName() {
    return firstName;
  }

  public void setFirstName(final String firstName) {
    this.firstName = firstName;
  }

  public String getLastName() {
    return lastName;
  }

  public void setLastName(final String lastName) {
    this.lastName = lastName;
  }

  public String getBirthDate() {
    return birthDate;
  }

  public void setBirthDate(final String birthDate) {
    this.birthDate = birthDate;
  }

  public String getValue() {
    return value;
  }

  public void setValue(String value) {
    this.value = value;
  }

  public List<String> getIpList() {
    return ipList;
  }

  public void setIpList(List<String> ipList) {
    this.ipList = ipList;
  }

  public BankDetails getBankDetails() {
    return bankDetails;
  }

  public void setBankDetails(BankDetails bankDetails) {
    this.bankDetails = bankDetails;
  }
}
