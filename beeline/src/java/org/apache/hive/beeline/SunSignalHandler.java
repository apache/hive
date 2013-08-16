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

/*
 * This source file is based on code taken from SQLLine 1.0.2
 * See SQLLine notice in LICENSE
 */
package org.apache.hive.beeline;

import java.sql.SQLException;
import java.sql.Statement;

import sun.misc.Signal;
import sun.misc.SignalHandler;

public class SunSignalHandler implements BeeLineSignalHandler, SignalHandler {
  private Statement stmt = null;

  SunSignalHandler () {
    // Interpret Ctrl+C as a request to cancel the currently
    // executing query.
    Signal.handle (new Signal ("INT"), this);
  }

  public void setStatement(Statement stmt) {
    this.stmt = stmt;
  }

  public void handle (Signal signal) {
    try {
      if (stmt != null) {
        stmt.cancel();
      }
    } catch (SQLException ex) {
      // ignore
    }
  }
}
