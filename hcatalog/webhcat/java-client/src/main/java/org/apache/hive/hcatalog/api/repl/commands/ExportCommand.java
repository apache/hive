/**
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
import org.apache.hive.hcatalog.api.repl.ReplicationUtils;
import org.apache.hive.hcatalog.data.ReaderWriter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ExportCommand implements Command {
  private String exportLocation;
  private String dbName = null;
  private String tableName = null;
  private Map<String, String> ptnDesc = null;
  private long eventId;
  private boolean isMetadataOnly = false;

  public ExportCommand(String dbName, String tableName, Map<String, String> ptnDesc,
                       String exportLocation, boolean isMetadataOnly, long eventId) {
    this.dbName = dbName;
    this.tableName = tableName;
    this.ptnDesc = ptnDesc;
    this.exportLocation = exportLocation;
    this.isMetadataOnly = isMetadataOnly;
    this.eventId = eventId;
  }

  /**
   * Trivial ctor to support Writable reflections instantiation
   * do not expect to use this object as-is, unless you call
   * readFields after using this ctor
   */
  public ExportCommand(){
  }

  @Override
  public List<String> get() {
    // EXPORT TABLE tablename [PARTITION (part_column="value"[, ...])]
    // TO 'export_target_path'
    StringBuilder sb = new StringBuilder();
    sb.append("EXPORT TABLE ");
    sb.append(dbName);
    sb.append(".");
    sb.append(tableName); // TODO: Handle quoted tablenames
    sb.append(ReplicationUtils.partitionDescriptor(ptnDesc));
    sb.append(" TO '");
    sb.append(exportLocation);
    sb.append("\' FOR ");
    if (isMetadataOnly){
      sb.append("METADATA ");
    }
    sb.append("REPLICATION(\'");
    sb.append(eventId);
    sb.append("\')");
    return Collections.singletonList(sb.toString());
  }

  @Override
  public boolean isRetriable() {
    return true; // Export is trivially retriable (after clearing out the staging dir provided.)
  }

  @Override
  public boolean isUndoable() {
    return true; // Export is trivially undoable - in that nothing needs doing to undo it.
  }

  @Override
  public List<String> getUndo() {
    return Collections.emptyList();
  }

  @Override
  public List<String> cleanupLocationsPerRetry() {
    return Collections.singletonList(exportLocation);
  }

  @Override
  public List<String> cleanupLocationsAfterEvent() {
    return Collections.singletonList(exportLocation);
  }

  @Override
  public long getEventId() {
    return eventId;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    ReaderWriter.writeDatum(dataOutput, dbName);
    ReaderWriter.writeDatum(dataOutput, tableName);
    ReaderWriter.writeDatum(dataOutput, ptnDesc);
    ReaderWriter.writeDatum(dataOutput, exportLocation);
    ReaderWriter.writeDatum(dataOutput, isMetadataOnly);
    ReaderWriter.writeDatum(dataOutput, eventId);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    dbName = (String)ReaderWriter.readDatum(dataInput);
    tableName = (String)ReaderWriter.readDatum(dataInput);
    ptnDesc = (Map<String,String>)ReaderWriter.readDatum(dataInput);
    exportLocation = (String)ReaderWriter.readDatum(dataInput);
    isMetadataOnly = (Boolean) ReaderWriter.readDatum(dataInput);
    eventId = (Long) ReaderWriter.readDatum(dataInput);
  }

}
