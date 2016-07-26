/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.beeline;

import org.apache.hive.jdbc.Utils;

import java.sql.SQLException;

/**
 * We need to update some client side information after executing some Hive Commands
 */
public class ClientCommandHookFactory {
  private final static ClientCommandHookFactory instance = new ClientCommandHookFactory();

  private ClientCommandHookFactory() {
  }

  public static ClientCommandHookFactory get() {
    return instance;
  }

  public class SetCommandHook extends ClientHook {

    public SetCommandHook(String sql) {
      super(sql);
    }

    @Override
    public void postHook(BeeLine beeLine) {
      if (!beeLine.isBeeLine()) {
        beeLine.getOpts().setHiveConf(beeLine.getCommands().getHiveConf(false));
      }
    }
  }

  public class UseCommandHook extends ClientHook {

    public UseCommandHook(String sql) {
      super(sql);
    }

    @Override
    public void postHook(BeeLine beeLine) {
      // Handler multi-line sql
      String line = sql.replaceAll("\\s+", " ");
      String strs[] = line.split(" ");
      String dbName;
      if (strs == null || strs.length != 2) {
        // unable to parse the use command
        dbName = "";
      } else {
        dbName = strs[1];
      }
      beeLine.setCurrentDatabase(dbName);
    }
  }

  public class ConnectCommandHook extends ClientHook {

    public ConnectCommandHook(String sql) {
      super(sql);
    }

    @Override
    public void postHook(BeeLine beeLine) {
      // Handler multi-line sql
      String line = sql.replaceAll("\\s+", " ");
      String strs[] = line.split(" ");
      String dbName;
      if (strs == null || strs.length < 1) {
        // unable to parse the connect command
        dbName = "";
      } else {
        try {
          dbName = Utils.parseURL(strs[1]).getDbName();
        } catch (Exception e) {
          // unable to parse the connect command
          dbName = "";
        }
      }
      beeLine.setCurrentDatabase(dbName);
    }
  }

  public class GoCommandHook extends ClientHook {

    public GoCommandHook(String sql) {
      super(sql);
    }

    @Override
    public void postHook(BeeLine beeLine) {
      String dbName = "";
      try {
        dbName = beeLine.getDatabaseConnection().getConnection().getSchema();
      } catch (SQLException e) {
        // unable to get the database, set the dbName empty
      }
      beeLine.setCurrentDatabase(dbName);
    }
  }

  public ClientHook getHook(BeeLine beeLine, String cmdLine) {
    if (!beeLine.isBeeLine()) {
      // In compatibility mode we need to hook to set, and use
      if (cmdLine.toLowerCase().startsWith("set")) {
        // Only set A = B command needs updating the configuration stored in client side.
        if (cmdLine.contains("=")) {
          return new SetCommandHook(cmdLine);
        } else {
          return null;
        }
      } else if (cmdLine.toLowerCase().startsWith("use")) {
        return new UseCommandHook(cmdLine);
      } else {
        return null;
      }
    } else {
      // In beeline mode we need to hook to use, connect, go, in case
      // the ShowDbInPrompt is set, so the database name is needed
      if (beeLine.getOpts().getShowDbInPrompt()) {
        if (cmdLine.toLowerCase().startsWith("use")) {
          return new UseCommandHook(cmdLine);
        } else if (cmdLine.toLowerCase().startsWith("connect")) {
          return new ConnectCommandHook(cmdLine);
        } else if (cmdLine.toLowerCase().startsWith("go")) {
          return new GoCommandHook(cmdLine);
        } else {
          return null;
        }
      } else {
        return null;
      }
    }
  }
}
