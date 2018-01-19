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

/**
 * This class is there to help testing, and to help initial development
 * and will be the default Command for NoopReplicationTask
 *
 * This is not intended to be a permanent class, and will likely move to the test
 * package after initial implementation.
 */

public class NoopCommand implements Command {
  private long eventId;

  /**
   * Trivial ctor to support Writable reflections instantiation
   * do not expect to use this object as-is, unless you call
   * readFields after using this ctor
   */
  public NoopCommand(){
  }

  public NoopCommand(long eventId){
    this.eventId = eventId;
  }

  @Override
  public List<String> get() {
    return Collections.emptyList();
  }

  @Override
  public boolean isRetriable() {
    return true;
  }

  @Override
  public boolean isUndoable() {
    return true;
  }

  @Override
  public List<String> getUndo() {
    return Collections.emptyList();
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
    ReaderWriter.writeDatum(dataOutput, eventId);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    eventId = (Long) ReaderWriter.readDatum(dataInput);
  }
}

