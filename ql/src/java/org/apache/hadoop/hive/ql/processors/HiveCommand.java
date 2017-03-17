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

package org.apache.hadoop.hive.ql.processors;

import java.util.HashSet;
import java.util.Set;

/*
 * HiveCommand is non-SQL statement such as setting a property or
 * adding a resource.
 **/
public enum HiveCommand {
  SET(),
  RESET(),
  DFS(),
  CRYPTO(true),
  ADD(),
  LIST(),
  RELOAD(),
  DELETE(),
  COMPILE();

  public static final boolean ONLY_FOR_TESTING = true;
  private boolean usedOnlyForTesting;

  HiveCommand() {
    this(false);
  }

  HiveCommand(boolean onlyForTesting) {
    this.usedOnlyForTesting = onlyForTesting;
  }

  public boolean isOnlyForTesting() {
    return this.usedOnlyForTesting;
  }

  private static final Set<String> COMMANDS = new HashSet<String>();
  static {
    for (HiveCommand command : HiveCommand.values()) {
      COMMANDS.add(command.name());
    }
  }

  public static HiveCommand find(String[] command) {
    return find(command, false);
  }

  public static HiveCommand find(String[] command, boolean findOnlyForTesting) {
    if (null == command){
      return null;
    }
    String cmd = command[0];
    if (cmd != null) {
      cmd = cmd.trim().toUpperCase();
      if (command.length > 1 && "role".equalsIgnoreCase(command[1])) {
        // special handling for set role r1 statement
        return null;
      } else if(command.length > 1 && "from".equalsIgnoreCase(command[1])) {
        //special handling for SQL "delete from <table> where..."
        return null;
      } else if(command.length > 1 && "set".equalsIgnoreCase(command[0]) && "autocommit".equalsIgnoreCase(command[1])) {
        return null;//don't want set autocommit true|false to get mixed with set hive.foo.bar...
      } else if (COMMANDS.contains(cmd)) {
        HiveCommand hiveCommand = HiveCommand.valueOf(cmd);

        if (findOnlyForTesting == hiveCommand.isOnlyForTesting()) {
          return hiveCommand;
        }

        return null;
      }
    }
    return null;
  }
}
