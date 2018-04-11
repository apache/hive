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

/*
 * This source file is based on code taken from SQLLine 1.0.2
 * See SQLLine notice in LICENSE
 */
package org.apache.hive.beeline;

import java.util.List;

import jline.console.completer.Completer;
import jline.console.completer.StringsCompleter;

class TableNameCompletor implements Completer {
  private final BeeLine beeLine;

  /**
   * @param beeLine
   */
  TableNameCompletor(BeeLine beeLine) {
    this.beeLine = beeLine;
  }

  public int complete(String buf, int pos, List cand) {
    if (beeLine.getDatabaseConnection() == null) {
      return -1;
    }
    return new StringsCompleter(beeLine.getDatabaseConnection().getTableNames(true))
        .complete(buf, pos, cand);
  }
}