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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;

import jline.SimpleCompletor;

class SQLCompletor extends SimpleCompletor {
  private final BeeLine beeLine;

  public SQLCompletor(BeeLine beeLine, boolean skipmeta)
      throws IOException, SQLException {
    super(new String[0]);
    this.beeLine = beeLine;

    Set<String> completions = new TreeSet<String>();

    // add the default SQL completions
    String keywords = new BufferedReader(new InputStreamReader(
        SQLCompletor.class.getResourceAsStream(
            "/sql-keywords.properties"))).readLine();

    // now add the keywords from the current connection
    try {
      keywords += "," + beeLine.getDatabaseConnection().getDatabaseMetaData().getSQLKeywords();
    } catch (Throwable t) {
    }
    try {
      keywords += "," + beeLine.getDatabaseConnection().getDatabaseMetaData().getStringFunctions();
    } catch (Throwable t) {
    }
    try {
      keywords += "," + beeLine.getDatabaseConnection().getDatabaseMetaData().getNumericFunctions();
    } catch (Throwable t) {
    }
    try {
      keywords += "," + beeLine.getDatabaseConnection().getDatabaseMetaData().getSystemFunctions();
    } catch (Throwable t) {
    }
    try {
      keywords += "," + beeLine.getDatabaseConnection().getDatabaseMetaData().getTimeDateFunctions();
    } catch (Throwable t) {
    }

    // also allow lower-case versions of all the keywords
    keywords += "," + keywords.toLowerCase();

    for (StringTokenizer tok = new StringTokenizer(keywords, ", "); tok.hasMoreTokens(); completions
        .add(tok.nextToken())) {
      ;
    }

    // now add the tables and columns from the current connection
    if (!(skipmeta)) {
      String[] columns = beeLine.getColumnNames(beeLine.getDatabaseConnection().getDatabaseMetaData());
      for (int i = 0; columns != null && i < columns.length; i++) {
        completions.add(columns[i++]);
      }
    }

    // set the Strings that will be completed
    setCandidateStrings(completions.toArray(new String[0]));
  }
}
