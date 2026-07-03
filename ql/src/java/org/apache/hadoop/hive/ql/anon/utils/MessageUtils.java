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

package org.apache.hadoop.hive.ql.anon.utils;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.anon.model.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hive.ql.anon.consts.MsgConst.*;

public final class MessageUtils {

  public static final Map<Class<? extends BaseMsg>, Integer> map = new HashMap<>();

  static {
    map.put(Msg1.class, MSG_MSG_1);
    map.put(Msg2.class, MSG_MSG_2);
    map.put(Msg3.class, MSG_MSG_3);
    map.put(Msg4.class, MSG_MSG_4);
  }

  public static Msg1 createMsg1(final long txnId) {
    final int len = 30;
    final String value = getRandomString(len, txnId);
    return new Msg1(txnId,
      getTestValue1(value),
      getTestValue2(value),
      getTestValue3(value)
    );
  }

  public static Msg2 createMsg2(final int userId) {
    final int len = 30;
    final String value = getRandomString(len, userId);
    return new Msg2(userId,
      getTestValue1(value),
      getTestValue2(value),
      getTestValue3(value)
    );
  }

  public static Msg3 createMsg3(final int userId, final int len) {
    final String value = getRandomString(len, userId);
    final BankDetails bankMsg = new BankDetails(
      getTestBankName(value),
      getTestCardNum(value),
      getTestPinCode(value)
    );
    return new Msg3(userId,
      getTestFirstName(value),
      getTestLastName(value),
      getTestAddress(value),
      getTestCity(value),
      getTestCountry(value),
      getTestEmail(value),
      getTestTelephone(value),
      getTestBirthdate(value),
      getTestValue(value),
      getTestIps(),
      bankMsg
    );
  }

  public static Msg4 createMsg4(final int userId) {
    final Msg4 m = new Msg4();
    m.setUserId(userId);
    final List<Address> addresses = new ArrayList<>();
    addresses.add(new Address("US", "1 Main"));
    addresses.add(new Address("DE", "2 Haupt"));
    m.setAddressBookList(addresses);
    return m;
  }

  public static List<String> getTestIps() {
    final List<String> ips = new ArrayList<>();
    ips.add("0.0.0.0");
    ips.add("127.0.0.1");
    ips.add("10.0.0.1");
    ips.add("192.168.1.1");
    return ips;
  }

  public static String getRandomString(final int len, final long id) {
    if (len < 10) {
      throw new IllegalArgumentException("len < 10");
    }
    String value = RandomStringUtils.randomAlphabetic(len) + "-" + id;
    value = StringUtils.right(value, len - 3);
    return value;
  }

  public static String getTestFirstName(final String value) {
    return PREFIX_FIRST_NAME + value;
  }

  public static String getTestLastName(final String value) {
    return PREFIX_LAST_NAME + value;
  }

  public static String getTestAddress(final String value) {
    return PREFIX_ADDRESS + value;
  }

  public static String getTestCity(final String value) {
    return PREFIX_CITY + value;
  }

  public static String getTestCountry(final String value) {
    return PREFIX_COUNTRY + value;
  }

  public static String getTestEmail(final String value) {
    return PREFIX_EMAIL + value;
  }

  public static String getTestTelephone(final String value) {
    return PREFIX_TELEPHONE + value;
  }

  public static String getTestBirthdate(final String value) {
    return PREFIX_BIRTHDATE + value;
  }

  public static String getTestValue(final String value) {
    return PREFIX_VALUE + value;
  }

  public static String getTestBankName(final String value) {
    return PREFIX_BANK_NAME + value;
  }

  public static String getTestCardNum(final String value) {
    return PREFIX_CARD_NUM + value;
  }

  public static String getTestPinCode(final String value) {
    return PREFIX_PIN_CODE + value;
  }

  public static String getTestValue1(final String value) {
    return PREFIX_VALUE1 + value;
  }

  public static String getTestValue2(final String value) {
    return PREFIX_VALUE2 + value;
  }

  public static String getTestValue3(final String value) {
    return PREFIX_VALUE3 + value;
  }
}
