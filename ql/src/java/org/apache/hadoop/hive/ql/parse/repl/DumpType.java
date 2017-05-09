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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.parse.repl;

import org.apache.hadoop.hive.ql.parse.repl.load.message.CreateFunctionHandler;
import org.apache.hadoop.hive.ql.parse.repl.load.message.DefaultHandler;
import org.apache.hadoop.hive.ql.parse.repl.load.message.DropPartitionHandler;
import org.apache.hadoop.hive.ql.parse.repl.load.message.DropTableHandler;
import org.apache.hadoop.hive.ql.parse.repl.load.message.InsertHandler;
import org.apache.hadoop.hive.ql.parse.repl.load.message.MessageHandler;
import org.apache.hadoop.hive.ql.parse.repl.load.message.RenamePartitionHandler;
import org.apache.hadoop.hive.ql.parse.repl.load.message.RenameTableHandler;
import org.apache.hadoop.hive.ql.parse.repl.load.message.TableHandler;
import org.apache.hadoop.hive.ql.parse.repl.load.message.TruncatePartitionHandler;
import org.apache.hadoop.hive.ql.parse.repl.load.message.TruncateTableHandler;

public enum DumpType {

  EVENT_CREATE_TABLE("EVENT_CREATE_TABLE") {
    @Override
    public MessageHandler handler() {
      return new TableHandler();
    }
  },
  EVENT_ADD_PARTITION("EVENT_ADD_PARTITION") {
    @Override
    public MessageHandler handler() {
      return new TableHandler();
    }
  },
  EVENT_DROP_TABLE("EVENT_DROP_TABLE") {
    @Override
    public MessageHandler handler() {
      return new DropTableHandler();
    }
  },
  EVENT_DROP_PARTITION("EVENT_DROP_PARTITION") {
    @Override
    public MessageHandler handler() {
      return new DropPartitionHandler();
    }
  },
  EVENT_ALTER_TABLE("EVENT_ALTER_TABLE") {
    @Override
    public MessageHandler handler() {
      return new TableHandler();
    }
  },
  EVENT_RENAME_TABLE("EVENT_RENAME_TABLE") {
    @Override
    public MessageHandler handler() {
      return new RenameTableHandler();
    }
  },
  EVENT_TRUNCATE_TABLE("EVENT_TRUNCATE_TABLE") {
    @Override
    public MessageHandler handler() {
      return new TruncateTableHandler();
    }
  },
  EVENT_ALTER_PARTITION("EVENT_ALTER_PARTITION") {
    @Override
    public MessageHandler handler() {
      return new TableHandler();
    }
  },
  EVENT_RENAME_PARTITION("EVENT_RENAME_PARTITION") {
    @Override
    public MessageHandler handler() {
      return new RenamePartitionHandler();
    }
  },
  EVENT_TRUNCATE_PARTITION("EVENT_TRUNCATE_PARTITION") {
    @Override
    public MessageHandler handler() {
      return new TruncatePartitionHandler();
    }
  },
  EVENT_INSERT("EVENT_INSERT") {
    @Override
    public MessageHandler handler() {
      return new InsertHandler();
    }
  },
  EVENT_CREATE_FUNCTION("EVENT_CREATE_FUNCTION") {
    @Override
    public MessageHandler handler() {
      return new CreateFunctionHandler();
    }
  },
  EVENT_UNKNOWN("EVENT_UNKNOWN") {
    @Override
    public MessageHandler handler() {
      return new DefaultHandler();
    }
  },
  BOOTSTRAP("BOOTSTRAP") {
    @Override
    public MessageHandler handler() {
      return new DefaultHandler();
    }
  },
  INCREMENTAL("INCREMENTAL") {
    @Override
    public MessageHandler handler() {
      return new DefaultHandler();
    }
  };

  String type = null;
  DumpType(String type) {
    this.type = type;
  }

  @Override
  public String toString(){
    return type;
  }

  public abstract MessageHandler handler();
}
