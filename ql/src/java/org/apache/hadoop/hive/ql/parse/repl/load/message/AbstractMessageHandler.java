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
package org.apache.hadoop.hive.ql.parse.repl.load.message;

import org.apache.hadoop.hive.metastore.messaging.MessageDeserializer;
import org.apache.hadoop.hive.metastore.messaging.MessageFactory;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

abstract class AbstractMessageHandler implements MessageHandler {
  final HashSet<ReadEntity> readEntitySet = new HashSet<>();
  final HashSet<WriteEntity> writeEntitySet = new HashSet<>();
  final Map<String, Long> tablesUpdated = new HashMap<>(),
      databasesUpdated = new HashMap<>();
  final MessageDeserializer deserializer = MessageFactory.getInstance().getDeserializer();

  @Override
  public Set<ReadEntity> readEntities() {
    return readEntitySet;
  }

  @Override
  public Set<WriteEntity> writeEntities() {
    return writeEntitySet;
  }

  @Override
  public Map<String, Long> tablesUpdated() {
    return tablesUpdated;
  }

  @Override
  public Map<String, Long> databasesUpdated() {
    return databasesUpdated;
  }

}
