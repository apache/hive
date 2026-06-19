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
package org.apache.hadoop.hive.ql.parse.repl;

import org.apache.hadoop.hive.ql.parse.repl.load.message.AddCheckConstraintHandler;
import org.apache.hadoop.hive.ql.parse.repl.load.message.AddDefaultConstraintHandler;
import org.apache.hadoop.hive.ql.parse.repl.load.message.AddNotNullConstraintHandler;
import org.apache.hadoop.hive.ql.parse.repl.load.message.AddForeignKeyHandler;
import org.apache.hadoop.hive.ql.parse.repl.load.message.AddPrimaryKeyHandler;
import org.apache.hadoop.hive.ql.parse.repl.load.message.AddUniqueConstraintHandler;
import org.apache.hadoop.hive.ql.parse.repl.load.message.AlterDatabaseHandler;
import org.apache.hadoop.hive.ql.parse.repl.load.message.CreateDatabaseHandler;
import org.apache.hadoop.hive.ql.parse.repl.load.message.CreateFunctionHandler;
import org.apache.hadoop.hive.ql.parse.repl.load.message.DefaultHandler;
import org.apache.hadoop.hive.ql.parse.repl.load.message.DropConstraintHandler;
import org.apache.hadoop.hive.ql.parse.repl.load.message.DropDatabaseHandler;
import org.apache.hadoop.hive.ql.parse.repl.load.message.DropFunctionHandler;
import org.apache.hadoop.hive.ql.parse.repl.load.message.DropPartitionHandler;
import org.apache.hadoop.hive.ql.parse.repl.load.message.DropTableHandler;
import org.apache.hadoop.hive.ql.parse.repl.load.message.InsertHandler;
import org.apache.hadoop.hive.ql.parse.repl.load.message.MessageHandler;
import org.apache.hadoop.hive.ql.parse.repl.load.message.RenamePartitionHandler;
import org.apache.hadoop.hive.ql.parse.repl.load.message.RenameTableHandler;
import org.apache.hadoop.hive.ql.parse.repl.load.message.TableHandler;
import org.apache.hadoop.hive.ql.parse.repl.load.message.TruncatePartitionHandler;
import org.apache.hadoop.hive.ql.parse.repl.load.message.TruncateTableHandler;
import org.apache.hadoop.hive.ql.parse.repl.load.message.OpenTxnHandler;
import org.apache.hadoop.hive.ql.parse.repl.load.message.CommitTxnHandler;
import org.apache.hadoop.hive.ql.parse.repl.load.message.AbortTxnHandler;
import org.apache.hadoop.hive.ql.parse.repl.load.message.AllocWriteIdHandler;
import org.apache.hadoop.hive.ql.parse.repl.load.message.UpdateTableColStatHandler;
import org.apache.hadoop.hive.ql.parse.repl.load.message.DeleteTableColStatHandler;
import org.apache.hadoop.hive.ql.parse.repl.load.message.UpdatePartColStatHandler;
import org.apache.hadoop.hive.ql.parse.repl.load.message.DeletePartColStatHandler;

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
  EVENT_DROP_FUNCTION("EVENT_DROP_FUNCTION") {
    @Override
    public MessageHandler handler() {
      return new DropFunctionHandler();
    }
  },
  EVENT_DROP_PARTITION("EVENT_DROP_PARTITION") {
    @Override
    public MessageHandler handler() {
      return new DropPartitionHandler();
    }
  },
  EVENT_ALTER_DATABASE("EVENT_ALTER_DATABASE") {
    @Override
    public MessageHandler handler() {
      return new AlterDatabaseHandler();
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
  EVENT_RENAME_DROP_TABLE("EVENT_RENAME_DROP_TABLE") {
    @Override
    public MessageHandler handler() {
      return new DropTableHandler();
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
  EVENT_ADD_PRIMARYKEY("EVENT_ADD_PRIMARYKEY") {
    @Override
    public MessageHandler handler() {
      return new AddPrimaryKeyHandler();
    }
  },
  EVENT_ADD_FOREIGNKEY("EVENT_ADD_FOREIGNKEY") {
    @Override
    public MessageHandler handler() {
      return new AddForeignKeyHandler();
    }
  },
  EVENT_ADD_UNIQUECONSTRAINT("EVENT_ADD_UNIQUECONSTRAINT") {
    @Override
    public MessageHandler handler() {
      return new AddUniqueConstraintHandler();
    }
  },
  EVENT_ADD_NOTNULLCONSTRAINT("EVENT_ADD_NOTNULLCONSTRAINT") {
    @Override
    public MessageHandler handler() {
      return new AddNotNullConstraintHandler();
    }
  },
  EVENT_ADD_DEFAULTCONSTRAINT("EVENT_ADD_DEFAULTCONSTRAINT") {
    @Override
    public MessageHandler handler() {
      return new AddDefaultConstraintHandler();
    }
  },
  EVENT_ADD_CHECKCONSTRAINT("EVENT_ADD_CHECKCONSTRAINT") {
    @Override
    public MessageHandler handler() {
      return new AddCheckConstraintHandler();
    }
  },
  EVENT_DROP_CONSTRAINT("EVENT_DROP_CONSTRAINT") {
    @Override
    public MessageHandler handler() {
      return new DropConstraintHandler();
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
  },
  PRE_OPTIMIZED_BOOTSTRAP("PRE_OPTIMIZED_BOOTSTRAP") {
    @Override
    public MessageHandler handler() {
      return new DefaultHandler();
    }
  },
  OPTIMIZED_BOOTSTRAP("OPTIMIZED_BOOTSTRAP") {
    @Override
    public MessageHandler handler() {
      return new DefaultHandler();
    }
  },
  EVENT_CREATE_DATABASE("EVENT_CREATE_DATABASE") {
    @Override
    public MessageHandler handler() {
      return new CreateDatabaseHandler();
    }
  },
  EVENT_DROP_DATABASE("EVENT_DROP_DATABASE") {
    @Override
    public MessageHandler handler() {
      return new DropDatabaseHandler();
    }
  },
  EVENT_OPEN_TXN("EVENT_OPEN_TXN") {
    @Override
    public MessageHandler handler() {
      return new OpenTxnHandler();
    }
  },
  EVENT_COMMIT_TXN("EVENT_COMMIT_TXN") {
    @Override
    public MessageHandler handler() {
      return new CommitTxnHandler();
    }
  },
  EVENT_ABORT_TXN("EVENT_ABORT_TXN") {
    @Override
    public MessageHandler handler() {
      return new AbortTxnHandler();
    }
  },
  EVENT_ALLOC_WRITE_ID("EVENT_ALLOC_WRITE_ID") {
    @Override
    public MessageHandler handler() {
      return new AllocWriteIdHandler();
    }
  },
  EVENT_UPDATE_TABLE_COL_STAT("EVENT_UPDATE_TABLE_COL_STAT") {
    @Override
    public MessageHandler handler() {
      return new UpdateTableColStatHandler();
    }
  },
  EVENT_DELETE_TABLE_COL_STAT("EVENT_DELETE_TABLE_COL_STAT") {
    @Override
    public MessageHandler handler() {
      return new DeleteTableColStatHandler();
    }
  },
  EVENT_UPDATE_PART_COL_STAT("EVENT_UPDATE_PART_COL_STAT") {
    @Override
    public MessageHandler handler() {
      return new UpdatePartColStatHandler();
    }
  },
  EVENT_DELETE_PART_COL_STAT("EVENT_DELETE_PART_COL_STAT") {
    @Override
    public MessageHandler handler() {
      return new DeletePartColStatHandler();
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
