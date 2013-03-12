/*
 *  Copyright (c) 2002,2003,2004,2005 Marc Prud'hommeaux
 *  All rights reserved.
 *
 *
 *  Redistribution and use in source and binary forms,
 *  with or without modification, are permitted provided
 *  that the following conditions are met:
 *
 *  Redistributions of source code must retain the above
 *  copyright notice, this list of conditions and the following
 *  disclaimer.
 *  Redistributions in binary form must reproduce the above
 *  copyright notice, this list of conditions and the following
 *  disclaimer in the documentation and/or other materials
 *  provided with the distribution.
 *  Neither the name of the <ORGANIZATION> nor the names
 *  of its contributors may be used to endorse or promote
 *  products derived from this software without specific
 *  prior written permission.
 *  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS
 *  AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED
 *  WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 *  WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 *  PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 *  OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 *  INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 *  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE
 *  GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
 *  BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY
 *  OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 *  OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING
 *  IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
 *  ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 *  This software is hosted by SourceForge.
 *  SourceForge is a trademark of VA Linux Systems, Inc.
 */

/*
 * This source file is based on code taken from SQLLine 1.0.2
 * The license above originally appeared in src/sqlline/SqlLine.java
 * http://sqlline.sourceforge.net/
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
            "sql-keywords.properties"))).readLine();

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
