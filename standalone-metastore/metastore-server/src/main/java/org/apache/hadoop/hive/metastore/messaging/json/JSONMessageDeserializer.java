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

package org.apache.hadoop.hive.metastore.messaging.json;

import com.fasterxml.jackson.core.json.JsonReadFeature;

import org.apache.hadoop.hive.metastore.messaging.AbortTxnMessage;
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
import org.apache.hadoop.hive.metastore.messaging.CommitCompactionMessage;
import org.apache.hadoop.hive.metastore.messaging.CommitTxnMessage;
import org.apache.hadoop.hive.metastore.messaging.CreateDatabaseMessage;
import org.apache.hadoop.hive.metastore.messaging.CreateFunctionMessage;
import org.apache.hadoop.hive.metastore.messaging.CreateTableMessage;
import org.apache.hadoop.hive.metastore.messaging.DropConstraintMessage;
import org.apache.hadoop.hive.metastore.messaging.DropDatabaseMessage;
import org.apache.hadoop.hive.metastore.messaging.DropFunctionMessage;
import org.apache.hadoop.hive.metastore.messaging.DropPartitionMessage;
import org.apache.hadoop.hive.metastore.messaging.DropTableMessage;
import org.apache.hadoop.hive.metastore.messaging.InsertMessage;
import org.apache.hadoop.hive.metastore.messaging.MessageDeserializer;
import org.apache.hadoop.hive.metastore.messaging.OpenTxnMessage;
import org.apache.hadoop.hive.metastore.messaging.AcidWriteMessage;
import org.apache.hadoop.hive.metastore.messaging.UpdateTableColumnStatMessage;
import org.apache.hadoop.hive.metastore.messaging.DeleteTableColumnStatMessage;
import org.apache.hadoop.hive.metastore.messaging.UpdatePartitionColumnStatMessage;
import org.apache.hadoop.hive.metastore.messaging.DeletePartitionColumnStatMessage;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * MessageDeserializer implementation, for deserializing from JSON strings.
 */
public class JSONMessageDeserializer extends MessageDeserializer {

  static ObjectMapper mapper = new ObjectMapper(); // Thread-safe.

  static {
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    mapper.configure(MapperFeature.AUTO_DETECT_GETTERS, false);
    mapper.configure(MapperFeature.AUTO_DETECT_IS_GETTERS, false);
    mapper.configure(MapperFeature.AUTO_DETECT_FIELDS, false);
    mapper.configure(JsonReadFeature.ALLOW_UNESCAPED_CONTROL_CHARS.mappedFeature(), true);
  }

  @Override
  public CreateDatabaseMessage getCreateDatabaseMessage(String messageBody) {
    try {
      return mapper.readValue(messageBody, JSONCreateDatabaseMessage.class);
    }
    catch (Exception exception) {
      throw new IllegalArgumentException("Could not construct JSONCreateDatabaseMessage.",
                                        exception);
    }
  }

  @Override
  public AlterDatabaseMessage getAlterDatabaseMessage(String messageBody) {
    try {
      return mapper.readValue(messageBody, JSONAlterDatabaseMessage.class);
    } catch (Exception exception) {
      throw new IllegalArgumentException("Could not construct JSONAlterDatabaseMessage.",
                                        exception);
    }
  }

  @Override
  public DropDatabaseMessage getDropDatabaseMessage(String messageBody) {
    try {
      return mapper.readValue(messageBody, JSONDropDatabaseMessage.class);
    }
    catch (Exception exception) {
      throw new IllegalArgumentException("Could not construct JSONDropDatabaseMessage.", exception);
    }
  }

  @Override
  public CreateTableMessage getCreateTableMessage(String messageBody) {
    try {
      return mapper.readValue(messageBody, JSONCreateTableMessage.class);
    }
    catch (Exception exception) {
      throw new IllegalArgumentException("Could not construct JSONCreateTableMessage.", exception);
    }
  }

  @Override
  public AlterTableMessage getAlterTableMessage(String messageBody) {
    try {
      return mapper.readValue(messageBody, JSONAlterTableMessage.class);
    }
    catch (Exception exception) {
      throw new IllegalArgumentException("Could not construct appropriate alter table type.",
          exception);
    }
  }

  @Override
  public DropTableMessage getDropTableMessage(String messageBody) {
    try {
      return mapper.readValue(messageBody, JSONDropTableMessage.class);
    }
    catch (Exception exception) {
      throw new IllegalArgumentException("Could not construct JSONDropTableMessage.", exception);
    }
  }

  @Override
  public AddPartitionMessage getAddPartitionMessage(String messageBody) {
    try {
      return mapper.readValue(messageBody, JSONAddPartitionMessage.class);
    }
    catch (Exception exception) {
      throw new IllegalArgumentException("Could not construct AddPartitionMessage.", exception);
    }
  }

  @Override
  public AlterPartitionMessage getAlterPartitionMessage(String messageBody) {
    try {
      return mapper.readValue(messageBody, JSONAlterPartitionMessage.class);
    } catch (Exception e) {
      throw new IllegalArgumentException("Could not construct AlterPartitionMessage.", e);
    }
  }

  @Override
  public AlterPartitionsMessage getAlterPartitionsMessage(String messageBody) {
    try {
      return mapper.readValue(messageBody, JSONAlterPartitionsMessage.class);
    } catch (Exception e) {
      throw new IllegalArgumentException("Could not construct AlterPartitionsMessage.", e);
    }
  }

  @Override
  public DropPartitionMessage getDropPartitionMessage(String messageBody) {
    try {
      return mapper.readValue(messageBody, JSONDropPartitionMessage.class);
    }
    catch (Exception exception) {
      throw new IllegalArgumentException("Could not construct DropPartitionMessage.", exception);
    }
  }

  @Override
  public CreateFunctionMessage getCreateFunctionMessage(String messageBody) {
    try {
      return mapper.readValue(messageBody, JSONCreateFunctionMessage.class);
    }
    catch (Exception exception) {
      throw new IllegalArgumentException("Could not construct JSONCreateFunctionMessage.",
                                        exception);
    }
  }

  @Override
  public DropFunctionMessage getDropFunctionMessage(String messageBody) {
    try {
      return mapper.readValue(messageBody, JSONDropFunctionMessage.class);
    }
    catch (Exception exception) {
      throw new IllegalArgumentException("Could not construct JSONDropDatabaseMessage.", exception);
    }
  }

  @Override
  public InsertMessage getInsertMessage(String messageBody) {
    try {
      return mapper.readValue(messageBody, JSONInsertMessage.class);
    } catch (Exception e) {
      throw new IllegalArgumentException("Could not construct InsertMessage", e);
    }
  }

  @Override
  public AddPrimaryKeyMessage getAddPrimaryKeyMessage(String messageBody) {
    try {
      return mapper.readValue(messageBody, JSONAddPrimaryKeyMessage.class);
    } catch (Exception e) {
      throw new IllegalArgumentException("Could not construct AddPrimaryKeyMessage", e);
    }
  }

  @Override
  public AddForeignKeyMessage getAddForeignKeyMessage(String messageBody) {
    try {
      return mapper.readValue(messageBody, JSONAddForeignKeyMessage.class);
    } catch (Exception e) {
      throw new IllegalArgumentException("Could not construct AddForeignKeyMessage", e);
    }
  }

  @Override
  public AddUniqueConstraintMessage getAddUniqueConstraintMessage(String messageBody) {
    try {
      return mapper.readValue(messageBody, JSONAddUniqueConstraintMessage.class);
    } catch (Exception e) {
      throw new IllegalArgumentException("Could not construct AddUniqueConstraintMessage", e);
    }
  }

  @Override
  public AddNotNullConstraintMessage getAddNotNullConstraintMessage(String messageBody) {
    try {
      return mapper.readValue(messageBody, JSONAddNotNullConstraintMessage.class);
    } catch (Exception e) {
      throw new IllegalArgumentException("Could not construct AddNotNullConstraintMessage", e);
    }
  }

  @Override
  public AddDefaultConstraintMessage getAddDefaultConstraintMessage(String messageBody) {
    try {
      return mapper.readValue(messageBody, JSONAddDefaultConstraintMessage.class);
    } catch (Exception e) {
      throw new IllegalArgumentException("Could not construct AddDefaultConstraintMessage", e);
    }
  }

  @Override
  public AddCheckConstraintMessage getAddCheckConstraintMessage(String messageBody) {
    try {
      return mapper.readValue(messageBody, JSONAddCheckConstraintMessage.class);
    } catch (Exception e) {
      throw new IllegalArgumentException("Could not construct AddCheckConstraintMessage", e);
    }
  }

  @Override
  public DropConstraintMessage getDropConstraintMessage(String messageBody) {
    try {
      return mapper.readValue(messageBody, JSONDropConstraintMessage.class);
    } catch (Exception e) {
      throw new IllegalArgumentException("Could not construct DropConstraintMessage", e);
    }
  }

  @Override
  public OpenTxnMessage getOpenTxnMessage(String messageBody) {
    try {
      return mapper.readValue(messageBody, JSONOpenTxnMessage.class);
    } catch (Exception e) {
      throw new IllegalArgumentException("Could not construct OpenTxnMessage", e);
    }
  }

  @Override
  public CommitTxnMessage getCommitTxnMessage(String messageBody) {
    try {
      return mapper.readValue(messageBody, JSONCommitTxnMessage.class);
    } catch (Exception e) {
      throw new IllegalArgumentException("Could not construct CommitTxnMessage", e);
    }
  }

  @Override
  public AbortTxnMessage getAbortTxnMessage(String messageBody) {
    try {
      return mapper.readValue(messageBody, JSONAbortTxnMessage.class);
    } catch (Exception e) {
      throw new IllegalArgumentException("Could not construct AbortTxnMessage", e);
    }
  }

  @Override
  public AllocWriteIdMessage getAllocWriteIdMessage(String messageBody) {
    try {
      return mapper.readValue(messageBody, JSONAllocWriteIdMessage.class);
    } catch (Exception e) {
      throw new IllegalArgumentException("Could not construct AllocWriteIdMessage", e);
    }
  }

  @Override
  public AcidWriteMessage getAcidWriteMessage(String messageBody) {
    try {
      return mapper.readValue(messageBody, JSONAcidWriteMessage.class);
    } catch (Exception e) {
      throw new IllegalArgumentException("Could not construct AcidWriteMessage", e);
    }
  }

  @Override
  public UpdateTableColumnStatMessage getUpdateTableColumnStatMessage(String messageBody) {
    try {
      return mapper.readValue(messageBody, JSONUpdateTableColumnStatMessage.class);
    } catch (Exception e) {
      throw new IllegalArgumentException("Could not construct UpdateTableColumnStatMessage", e);
    }
  }

  @Override
  public DeleteTableColumnStatMessage getDeleteTableColumnStatMessage(String messageBody) {
    try {
      return mapper.readValue(messageBody, JSONDeleteTableColumnStatMessage.class);
    } catch (Exception e) {
      throw new IllegalArgumentException("Could not construct UpdateTableColumnStatMessage", e);
    }
  }

  @Override
  public UpdatePartitionColumnStatMessage getUpdatePartitionColumnStatMessage(String messageBody) {
    try {
      return mapper.readValue(messageBody, JSONUpdatePartitionColumnStatMessage.class);
    } catch (Exception e) {
      throw new IllegalArgumentException("Could not construct UpdatePartitionColumnStatMessage", e);
    }
  }

  @Override
  public DeletePartitionColumnStatMessage getDeletePartitionColumnStatMessage(String messageBody) {
    try {
      return mapper.readValue(messageBody, JSONDeletePartitionColumnStatMessage.class);
    } catch (Exception e) {
      throw new IllegalArgumentException("Could not construct UpdatePartitionColumnStatMessage", e);
    }
  }

  public CommitCompactionMessage getCommitCompactionMessage(String messageBody) {
    try {
      return mapper.readValue(messageBody, JSONCommitCompactionMessage.class);
    } catch (Exception e) {
      throw new IllegalArgumentException("Could not construct CommitCompactionMessage", e);
    }
  }
}
