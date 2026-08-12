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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hive.metastore.messaging.json.gzip;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hive.metastore.messaging.AbortTxnMessage;
import org.apache.hadoop.hive.metastore.messaging.AcidWriteMessage;
import org.apache.hadoop.hive.metastore.messaging.AddCheckConstraintMessage;
import org.apache.hadoop.hive.metastore.messaging.AddDefaultConstraintMessage;
import org.apache.hadoop.hive.metastore.messaging.AddForeignKeyMessage;
import org.apache.hadoop.hive.metastore.messaging.AddNotNullConstraintMessage;
import org.apache.hadoop.hive.metastore.messaging.AddPartitionMessage;
import org.apache.hadoop.hive.metastore.messaging.AddPrimaryKeyMessage;
import org.apache.hadoop.hive.metastore.messaging.AddUniqueConstraintMessage;
import org.apache.hadoop.hive.metastore.messaging.AllocWriteIdMessage;
import org.apache.hadoop.hive.metastore.messaging.AlterDatabaseMessage;
import org.apache.hadoop.hive.metastore.messaging.AlterPartitionMessage;
import org.apache.hadoop.hive.metastore.messaging.AlterPartitionsMessage;
import org.apache.hadoop.hive.metastore.messaging.AlterTableMessage;
import org.apache.hadoop.hive.metastore.messaging.CommitTxnMessage;
import org.apache.hadoop.hive.metastore.messaging.CommitCompactionMessage;
import org.apache.hadoop.hive.metastore.messaging.CreateDatabaseMessage;
import org.apache.hadoop.hive.metastore.messaging.CreateFunctionMessage;
import org.apache.hadoop.hive.metastore.messaging.CreateTableMessage;
import org.apache.hadoop.hive.metastore.messaging.DropConstraintMessage;
import org.apache.hadoop.hive.metastore.messaging.DropDatabaseMessage;
import org.apache.hadoop.hive.metastore.messaging.DropFunctionMessage;
import org.apache.hadoop.hive.metastore.messaging.DropPartitionMessage;
import org.apache.hadoop.hive.metastore.messaging.DropTableMessage;
import org.apache.hadoop.hive.metastore.messaging.InsertMessage;
import org.apache.hadoop.hive.metastore.messaging.OpenTxnMessage;
import org.apache.hadoop.hive.metastore.messaging.UpdateTableColumnStatMessage;
import org.apache.hadoop.hive.metastore.messaging.UpdatePartitionColumnStatMessage;
import org.apache.hadoop.hive.metastore.messaging.DeleteTableColumnStatMessage;
import org.apache.hadoop.hive.metastore.messaging.DeletePartitionColumnStatMessage;
import org.apache.hadoop.hive.metastore.messaging.json.JSONMessageDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.zip.GZIPInputStream;


public class DeSerializer extends JSONMessageDeserializer {
  private static final Logger LOG = LoggerFactory.getLogger(Serializer.class.getName());

  private static String deCompress(String messageBody) {
    byte[] decodedBytes = Base64.getDecoder().decode(messageBody.getBytes(StandardCharsets.UTF_8));
    try (
        ByteArrayInputStream in = new ByteArrayInputStream(decodedBytes);
        GZIPInputStream is = new GZIPInputStream(in)
    ) {
      byte[] bytes = IOUtils.toByteArray(is);
      return new String(bytes, StandardCharsets.UTF_8);
    } catch (IOException e) {
      LOG.error("cannot decode the stream", e);
      LOG.debug("base64 encoded String", messageBody);
      throw new RuntimeException("cannot decode the stream ", e);
    }
  }

  /**
   * this is mainly as a utility to allow debugging of messages for developers by providing the
   * message in a file and getting an actual message out.
   * This class on a deployed hive instance will also be bundled in hive-exec jar.
   *
   */
  public static void main(String[] args) throws IOException {
    if(args.length != 1) {
      System.out.println("Usage:");
      System.out.println("java -cp [classpath] "+DeSerializer.class.getCanonicalName() +" [file_location]");
    }
    System.out.print(
        deCompress(FileUtils.readFileToString(new File(args[0]), StandardCharsets.UTF_8)));
  }

  @Override
  public CreateDatabaseMessage getCreateDatabaseMessage(String messageBody) {
    return super.getCreateDatabaseMessage(deCompress(messageBody));
  }

  @Override
  public AlterDatabaseMessage getAlterDatabaseMessage(String messageBody) {
    return super.getAlterDatabaseMessage(deCompress(messageBody));
  }

  @Override
  public DropDatabaseMessage getDropDatabaseMessage(String messageBody) {
    return super.getDropDatabaseMessage(deCompress(messageBody));
  }

  @Override
  public CreateTableMessage getCreateTableMessage(String messageBody) {
    return super.getCreateTableMessage(deCompress(messageBody));
  }

  @Override
  public AlterTableMessage getAlterTableMessage(String messageBody) {
    return super.getAlterTableMessage(deCompress(messageBody));
  }

  @Override
  public DropTableMessage getDropTableMessage(String messageBody) {
    return super.getDropTableMessage(deCompress(messageBody));
  }

  @Override
  public AddPartitionMessage getAddPartitionMessage(String messageBody) {
    return super.getAddPartitionMessage(deCompress(messageBody));
  }

  @Override
  public AlterPartitionMessage getAlterPartitionMessage(String messageBody) {
    return super.getAlterPartitionMessage(deCompress(messageBody));
  }

  @Override
  public AlterPartitionsMessage getAlterPartitionsMessage(String messageBody) {
    return super.getAlterPartitionsMessage(deCompress(messageBody));
  }

  @Override
  public DropPartitionMessage getDropPartitionMessage(String messageBody) {
    return super.getDropPartitionMessage(deCompress(messageBody));
  }

  @Override
  public CreateFunctionMessage getCreateFunctionMessage(String messageBody) {
    return super.getCreateFunctionMessage(deCompress(messageBody));
  }

  @Override
  public DropFunctionMessage getDropFunctionMessage(String messageBody) {
    return super.getDropFunctionMessage(deCompress(messageBody));
  }

  @Override
  public InsertMessage getInsertMessage(String messageBody) {
    return super.getInsertMessage(deCompress(messageBody));
  }

  @Override
  public AddPrimaryKeyMessage getAddPrimaryKeyMessage(String messageBody) {
    return super.getAddPrimaryKeyMessage(deCompress(messageBody));
  }

  @Override
  public AddForeignKeyMessage getAddForeignKeyMessage(String messageBody) {
    return super.getAddForeignKeyMessage(deCompress(messageBody));
  }

  @Override
  public AddUniqueConstraintMessage getAddUniqueConstraintMessage(String messageBody) {
    return super.getAddUniqueConstraintMessage(deCompress(messageBody));
  }

  @Override
  public AddDefaultConstraintMessage getAddDefaultConstraintMessage(String messageBody) {
    return super.getAddDefaultConstraintMessage(deCompress(messageBody));
  }

  @Override
  public AddNotNullConstraintMessage getAddNotNullConstraintMessage(String messageBody) {
    return super.getAddNotNullConstraintMessage(deCompress(messageBody));
  }

  @Override
  public AddCheckConstraintMessage getAddCheckConstraintMessage(String messageBody) {
    return super.getAddCheckConstraintMessage(deCompress(messageBody));
  }

  @Override
  public DropConstraintMessage getDropConstraintMessage(String messageBody) {
    return super.getDropConstraintMessage(deCompress(messageBody));
  }

  @Override
  public OpenTxnMessage getOpenTxnMessage(String messageBody) {
    return super.getOpenTxnMessage(deCompress(messageBody));
  }

  @Override
  public CommitTxnMessage getCommitTxnMessage(String messageBody) {
    return super.getCommitTxnMessage(deCompress(messageBody));
  }

  @Override
  public AbortTxnMessage getAbortTxnMessage(String messageBody) {
    return super.getAbortTxnMessage(deCompress(messageBody));
  }

  @Override
  public AllocWriteIdMessage getAllocWriteIdMessage(String messageBody) {
    return super.getAllocWriteIdMessage(deCompress(messageBody));
  }

  @Override
  public CommitCompactionMessage getCommitCompactionMessage(String messageBody) {
    return super.getCommitCompactionMessage(deCompress(messageBody));
  }

  @Override
  public AcidWriteMessage getAcidWriteMessage(String messageBody) {
    return super.getAcidWriteMessage(deCompress(messageBody));
  }

  @Override
  public UpdateTableColumnStatMessage getUpdateTableColumnStatMessage(String messageBody) {
    return super.getUpdateTableColumnStatMessage(deCompress(messageBody));
  }

  @Override
  public UpdatePartitionColumnStatMessage getUpdatePartitionColumnStatMessage(String messageBody) {
    return super.getUpdatePartitionColumnStatMessage(deCompress(messageBody));
  }

  @Override
  public DeleteTableColumnStatMessage getDeleteTableColumnStatMessage(String messageBody) {
    return super.getDeleteTableColumnStatMessage(deCompress(messageBody));
  }

  @Override
  public DeletePartitionColumnStatMessage getDeletePartitionColumnStatMessage(String messageBody) {
    return super.getDeletePartitionColumnStatMessage(deCompress(messageBody));
  }

  public String deSerializeGenericString(String messageBody) {
    return deCompress(messageBody);
  }
}
