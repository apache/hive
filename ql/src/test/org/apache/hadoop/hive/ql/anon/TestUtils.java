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

package org.apache.hadoop.hive.ql.anon;

import javolution.testing.AssertionException;
import org.apache.hadoop.hive.metastore.api.ColumnInternalFormat;
import static org.apache.hadoop.hive.ql.anon.consts.MsgConst.MSG_MSG_4;
import org.apache.hadoop.hive.ql.anon.builders.InsertStatementBuilder;
import org.apache.hadoop.hive.ql.anon.convert.AvroBodyConverter;
import org.apache.hadoop.hive.ql.anon.convert.JsonBodyConverter;
import org.apache.hadoop.hive.ql.anon.convert.MsgpackBodyConverter;
import org.apache.hadoop.hive.ql.anon.convert.ProtobufBodyConverter;
import org.apache.hadoop.hive.ql.anon.convert.XmlBodyConverter;
import org.apache.hadoop.hive.ql.anon.policy.DataErasurePolicy;
import org.apache.hadoop.hive.ql.anon.utils.AvroUtils;
import org.apache.hadoop.hive.ql.anon.utils.MessageUtils;
import org.apache.hadoop.hive.ql.anon.model.Msg3;
import org.apache.hadoop.hive.ql.anon.utils.ProtoUtils;
import org.apache.hadoop.hive.ql.anon.proto.TestMessages;
import org.junit.jupiter.api.Assertions;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public final class TestUtils {

  public static final String HOME_DIR = System.getenv("HOME");
  public static final String WH_DIR = "/opt/hive/";
  public static final String TMP_DIR = HOME_DIR + "/tmp/";

  static {
    createDir(WH_DIR);
    createDir(TMP_DIR);
  }

  private static void createDir(final String dir) {
    Path dirPath = Paths.get(dir);
    if (!Files.exists(dirPath)) {
      try {
        Files.createDirectories(dirPath);
      } catch (IOException e) {
        throw new TestException("bad setup 2", e);
      }
    }
  }

  public static void validateRowSet(final List lst, final ColumnInternalFormat internalFormat, final int userId) {
    for (final Object o : lst) {
      final String row = (String) o;
      final String[] cols = row.split("\\t");
      if (cols.length != 3) {
        continue;
      }
      final String colValue = cols[2];
      if (internalFormat == ColumnInternalFormat.PROTOBUF) {
        final TestMessages.Msg3 msg3 = convert2(colValue, internalFormat);
        if (msg3.getUserId() == userId) {
          Assertions.assertEquals("", msg3.getCountry());
          Assertions.assertEquals("", msg3.getCity());
          Assertions.assertEquals("", msg3.getTelephone());
          for (final String ip : msg3.getIpList()) {
            Assertions.assertEquals("", ip);
          }
        } else {
          Assertions.assertNotEquals("", msg3.getCity());
          Assertions.assertNotEquals("", msg3.getCountry());
          Assertions.assertNotEquals("", msg3.getTelephone());
        }
      } else if (internalFormat == ColumnInternalFormat.AVRO) {
        final org.apache.hadoop.hive.ql.anon.avro.Msg3 msg3 = convert3(colValue, internalFormat);
        if (msg3.getUserId() == userId) {
          Assertions.assertEquals("", msg3.getCountry());
          Assertions.assertEquals("", msg3.getCity());
          Assertions.assertEquals("", msg3.getTelephone());
          for (final String ip : msg3.getIpList()) {
            Assertions.assertEquals("", ip);
          }
        } else {
          Assertions.assertNotEquals("", msg3.getCity());
          Assertions.assertNotEquals("", msg3.getCountry());
          Assertions.assertNotEquals("", msg3.getTelephone());
        }
      } else {
        final Msg3 msg3 = convert(colValue, internalFormat);
        if (msg3.getUserId() == userId) {
          Assertions.assertEquals("", msg3.getCountry());
          Assertions.assertEquals("", msg3.getCity());
          Assertions.assertEquals("", msg3.getTelephone());
          for (final String ip : msg3.getIpList()) {
            Assertions.assertEquals("", ip);
          }
        } else {
          Assertions.assertNotEquals("", msg3.getCity());
          Assertions.assertNotEquals("", msg3.getCountry());
          Assertions.assertNotEquals("", msg3.getTelephone());
        }

      }
    }
  }

  private static Msg3 convert(final String value, final ColumnInternalFormat internalFormat) {
    switch (internalFormat) {
      case JSON:
        return JsonBodyConverter.convert(value, Msg3.class);
      case MSGPACK:
        return MsgpackBodyConverter.convert(value, Msg3.class);
      case XML:
        return XmlBodyConverter.convert(value, Msg3.class);
      default:
        throw new AssertionException("bad format: " + internalFormat.name());
    }
  }

  private static TestMessages.Msg3 convert2(final String value, final ColumnInternalFormat internalFormat) {
    if (internalFormat == ColumnInternalFormat.PROTOBUF) {
      return ProtobufBodyConverter.convert(value, TestMessages.Msg3.class);
    }
    throw new AssertionException("bad format: " + internalFormat.name());
  }

  private static org.apache.hadoop.hive.ql.anon.avro.Msg3 convert3(final String value, final ColumnInternalFormat internalFormat) {
    if (internalFormat == ColumnInternalFormat.AVRO) {
      return (org.apache.hadoop.hive.ql.anon.avro.Msg3) AvroBodyConverter.convert(value, org.apache.hadoop.hive.ql.anon.avro.Msg3.class);
    }
    throw new AssertionException("bad format: " + internalFormat.name());
  }

  public static String getTestPolicyDsl() {
    final Path policyPath = Paths.get("./src/test/resources_anon/erp/policy.erp");
    try {
      return String.join("\n", Files.readAllLines(policyPath));
    } catch (IOException e) {
      throw new TestException(e);
    }
  }

  public static DataErasurePolicy getTestPolicy() {
    return DataErasurePolicy.fromDsl(getTestPolicyDsl());
  }

  public static List<String> getInsertCommands(final String tblName, final int userId, final int len, final ColumnInternalFormat internalFormat) {

    final int otherUserId = userId + 1;
    final TestBodyType bodyType = TestUtils.getBodyType(internalFormat);
    final List<String> insertCommands = new ArrayList<>();

    final String bodyColValue = getSerializedMessage(internalFormat, userId, len);
    final String bodyColValue2 = getSerializedMessage(internalFormat, otherUserId, len);

    String cmd = new InsertStatementBuilder(1, tblName, 3, 111, bodyColValue, bodyType).build();
    String cmd2 = new InsertStatementBuilder(2, tblName, 3, 121, bodyColValue, bodyType).build();
    String cmd3 = new InsertStatementBuilder(3, tblName, 3, 131, bodyColValue, bodyType).build();
    String cmd4 = new InsertStatementBuilder(1, tblName, 3, 141, bodyColValue2, bodyType).build();

    insertCommands.add(cmd);
    insertCommands.add(cmd2);
    insertCommands.add(cmd3);
    insertCommands.add(cmd4);

    return insertCommands;
  }

  public static List<String> getInsertCommandsMsg4(final String tblName, final int targetUserId,
                                                   final int otherUserId, final ColumnInternalFormat internalFormat) {
    final TestBodyType bodyType = TestUtils.getBodyType(internalFormat);
    final List<String> insertCommands = new ArrayList<>();

    final String targetBody = getSerializedMessageMsg4(internalFormat, targetUserId);
    final String otherBody = getSerializedMessageMsg4(internalFormat, otherUserId);

    insertCommands.add(new InsertStatementBuilder(1, tblName, MSG_MSG_4, 211, targetBody, bodyType).build());
    insertCommands.add(new InsertStatementBuilder(1, tblName, MSG_MSG_4, 221, targetBody, bodyType).build());
    insertCommands.add(new InsertStatementBuilder(1, tblName, MSG_MSG_4, 231, otherBody, bodyType).build());

    return insertCommands;
  }

  private static String getSerializedMessageMsg4(final ColumnInternalFormat internalFormat, final int userId) {
    switch (internalFormat) {
      case JSON: {
        return JsonBodyConverter.serializeMsg(MessageUtils.createMsg4(userId));
      }
      case MSGPACK: {
        return MsgpackBodyConverter.serializeMsgToHex(MessageUtils.createMsg4(userId));
      }
      case XML: {
        return XmlBodyConverter.serializeMsg(MessageUtils.createMsg4(userId));
      }
      case PROTOBUF: {
        return ProtobufBodyConverter.serializeMsgToHex(ProtoUtils.createMsg4(userId));
      }
      case AVRO: {
        return AvroBodyConverter.serializeMsgToHex(AvroUtils.createMsg4(userId));
      }
      default: {
        throw new AssertionException("bad format: " + internalFormat.name());
      }
    }
  }

  public static TestBodyType getBodyType(final ColumnInternalFormat internalFormat) {
    switch (internalFormat) {
      case AVRO:
      case PROTOBUF:
      case MSGPACK: {
        return TestBodyType.BINARY;
      }
      case JSON:
      case XML: {
        return TestBodyType.STRING;
      }
      default: {
        throw new UnsupportedOperationException("Unsupported internal format: " + internalFormat);
      }
    }
  }

  private static String getSerializedMessage(final ColumnInternalFormat internalFormat, final int userId, final int len) {
    switch (internalFormat) {
      case JSON: {
        return JsonBodyConverter.serializeMsg(MessageUtils.createMsg3(userId, len));
      }
      case MSGPACK: {
        return MsgpackBodyConverter.serializeMsgToHex(MessageUtils.createMsg3(userId, len));
      }
      case XML: {
        return XmlBodyConverter.serializeMsg(MessageUtils.createMsg3(userId, len));
      }
      case PROTOBUF: {
        return ProtobufBodyConverter.serializeMsgToHex(ProtoUtils.createMsg3(userId, len));
      }
      case AVRO: {
        return AvroBodyConverter.serializeMsgToHex(AvroUtils.createMsg3(userId, len));
      }
      default: {
        throw new AssertionException("bad format: " + internalFormat.name());
      }
    }
  }
}
