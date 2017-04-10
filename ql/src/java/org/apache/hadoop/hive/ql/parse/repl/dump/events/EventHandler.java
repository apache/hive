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
package org.apache.hadoop.hive.ql.parse.repl.dump.events;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;

import org.apache.hadoop.hive.ql.parse.repl.load.DumpMetaData;
import org.apache.hadoop.hive.ql.parse.repl.DumpType;

public interface EventHandler {
  void handle(Context withinContext) throws Exception;

  long fromEventId();

  long toEventId();

  DumpType dumpType();

  class Context {
    final Path eventRoot, cmRoot;
    final Hive db;
    final HiveConf hiveConf;
    final ReplicationSpec replicationSpec;

    public Context(Path eventRoot, Path cmRoot, Hive db, HiveConf hiveConf,
        ReplicationSpec replicationSpec) {
      this.eventRoot = eventRoot;
      this.cmRoot = cmRoot;
      this.db = db;
      this.hiveConf = hiveConf;
      this.replicationSpec = replicationSpec;
    }

    DumpMetaData createDmd(EventHandler eventHandler) {
      return new DumpMetaData(
          eventRoot,
          eventHandler.dumpType(),
          eventHandler.fromEventId(),
          eventHandler.toEventId(),
          cmRoot, hiveConf
      );
    }
  }
}
