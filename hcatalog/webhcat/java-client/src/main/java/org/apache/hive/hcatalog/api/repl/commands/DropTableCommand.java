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
package org.apache.hive.hcatalog.api.repl.commands;

import org.apache.hive.hcatalog.api.repl.Command;
import org.apache.hive.hcatalog.data.ReaderWriter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class DropTableCommand implements Command {
  private long eventId;
  private String dbName = null;
  private String tableName = null;
  private boolean isReplicatedEvent = false;

  public DropTableCommand(String dbName, String tableName, boolean isReplicatedEvent, long eventId) {
    this.dbName = dbName;
    this.tableName = tableName;
    this.isReplicatedEvent = isReplicatedEvent;
    this.eventId = eventId;
  }

  /**
   * Trivial ctor to support Writable reflections instantiation
   * do not expect to use this object as-is, unless you call
   * readFields after using this ctor
   */
  public DropTableCommand() {
  }

   @Override
  public List<String> get() {
    // DROP TABLE [IF EXISTS] table_name;
    StringBuilder sb = new StringBuilder();
    sb.append("DROP TABLE IF EXISTS ");
    sb.append(dbName);
    sb.append('.');
    sb.append(tableName); // TODO: Handle quoted tablenames
    if (isReplicatedEvent){
      sb.append(" FOR REPLICATION(\'");
      sb.append(eventId);
      sb.append("\')");
    }
    return Collections.singletonList(sb.toString());
  }

  @Override
  public boolean isRetriable() {
    return true;
  }

  @Override
  public boolean isUndoable() {
    return false;
  }

  @Override
  public List<String> getUndo() {
    throw new UnsupportedOperationException("getUndo called on command that does returned false for isUndoable");
  }

  @Override
  public List<String> cleanupLocationsPerRetry() {
    return Collections.emptyList();
  }

  @Override
  public List<String> cleanupLocationsAfterEvent() {
    return Collections.emptyList();
  }

  @Override
  public long getEventId() {
    return eventId;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    ReaderWriter.writeDatum(dataOutput, dbName);
    ReaderWriter.writeDatum(dataOutput, tableName);
    ReaderWriter.writeDatum(dataOutput, isReplicatedEvent);
    ReaderWriter.writeDatum(dataOutput, eventId);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    dbName = (String)ReaderWriter.readDatum(dataInput);
    tableName = (String)ReaderWriter.readDatum(dataInput);
    isReplicatedEvent = (Boolean) ReaderWriter.readDatum(dataInput);
    eventId = (Long) ReaderWriter.readDatum(dataInput);
  }

}
