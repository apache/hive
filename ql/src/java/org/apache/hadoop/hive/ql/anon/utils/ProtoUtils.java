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

import com.google.protobuf.GeneratedMessageV3;
import org.apache.hadoop.hive.ql.anon.proto.TestMessages;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hive.ql.anon.consts.MsgConst.*;
import static org.apache.hadoop.hive.ql.anon.utils.MessageUtils.*;

public final class ProtoUtils {

  public static final Map<Class<? extends GeneratedMessageV3>, Integer> map = new HashMap<>();

  static {
    map.put(TestMessages.Msg1.class, MSG_MSG_1);
    map.put(TestMessages.Msg2.class, MSG_MSG_2);
    map.put(TestMessages.Msg3.class, MSG_MSG_3);
    map.put(TestMessages.Msg4.class, MSG_MSG_4);
  }

  public static boolean contains(final Class<?> clazz, final String fieldName) {
    Field[] fields = clazz.getDeclaredFields();
    for (Field field : fields) {
      int modifiers = field.getModifiers();
      if (Modifier.isStatic(modifiers) || !Modifier.isPrivate(modifiers)) {
        continue;
      }
      if (fieldName.equals(field.getName())) {
        return true;
      }
      Class<?> type = field.getType();
      if (GeneratedMessageV3.class.isAssignableFrom(type)) {
        return contains(type, fieldName);
      }
      if (List.class.isAssignableFrom(type)) {
        ParameterizedType gt = (ParameterizedType) field.getGenericType();
        Type at = gt.getActualTypeArguments()[0];
        Class<?> cat = (Class<?>) at;
        if (GeneratedMessageV3.class.isAssignableFrom(cat)) {
          return contains(cat, fieldName);
        }
      }
    }
    return false;
  }

  public static TestMessages.Msg1 createMsg1(final long txnId) {
    final int len = 30;
    final String value = getRandomString(len, txnId);
    return TestMessages.Msg1
      .newBuilder()
      .setTxnId(txnId)
      .setValue1(getTestValue1(value))
      .setValue2(getTestValue2(value))
      .setValue3(getTestValue3(value))
      .build();
  }

  public static TestMessages.Msg2 createMsg2(final int userId) {
    final int len = 30;
    final String value = getRandomString(len, userId);
    return TestMessages.Msg2
      .newBuilder()
      .setUserId(userId)
      .setValue1(getTestValue1(value))
      .setValue2(getTestValue2(value))
      .setValue3(getTestValue3(value))
      .build();
  }

  public static TestMessages.Msg3 createMsg3(final int userId, final int len) {
    final String value = getRandomString(len, userId);
    final TestMessages.Msg3.Builder builder = TestMessages.Msg3
      .newBuilder()
      .setUserId(userId)
      .setFirstName(getTestFirstName(value))
      .setLastName(getTestLastName(value))
      .setAddress(getTestAddress(value))
      .setCity(getTestCity(value))
      .setCountry(getTestCountry(value))
      .setEmail(getTestEmail(value))
      .setTelephone(getTestTelephone(value))
      .setBirthDate(getTestBirthdate(value))
      .setValue(getTestValue(value))
      .setBankDetails(
        TestMessages
          .BankDetails
          .newBuilder()
          .setBankName(getTestBankName(value))
          .setCardNum(getTestCardNum(value))
          .setPinCode(getTestPinCode(value))
          .build()
      );

    final List<String> ips = getTestIps();
    for (final String ip : ips) {
      builder.addIp(ip);
    }

    return builder.build();
  }

  public static TestMessages.Msg4 createMsg4(final int userId) {
    return TestMessages.Msg4
      .newBuilder()
      .setUserId(userId)
      .addAddressBook(TestMessages.Address.newBuilder().setCountry("US").setStreet("1 Main").build())
      .addAddressBook(TestMessages.Address.newBuilder().setCountry("DE").setStreet("2 Haupt").build())
      .build();
  }

}
