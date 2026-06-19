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
package org.apache.hadoop.hive.ql.exec.repl;

/**
 * ReplAck, used for repl acknowledgement constants.
 */
public enum ReplAck {
    DUMP_ACKNOWLEDGEMENT("_finished_dump"),
    EVENTS_DUMP("_events_dump"),
    LOAD_ACKNOWLEDGEMENT("_finished_load"),
    NON_RECOVERABLE_MARKER("_non_recoverable"),
    LOAD_METADATA("_load_metadata"),
    FAILOVER_READY_MARKER("_failover_ready");
  private String ack;
  ReplAck(String ack) {
    this.ack = ack;
  }

  @Override
  public String toString() {
    return ack;
  }
}
