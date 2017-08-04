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
package org.apache.hadoop.hive.ql.parse.repl.dump;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.io.IOUtils;

import com.google.common.collect.Collections2;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

public class Utils {
  public static void writeOutput(List<String> values, Path outputFile, HiveConf hiveConf)
      throws SemanticException {
    DataOutputStream outStream = null;
    try {
      FileSystem fs = outputFile.getFileSystem(hiveConf);
      outStream = fs.create(outputFile);
      outStream.writeBytes((values.get(0) == null ? Utilities.nullStringOutput : values.get(0)));
      for (int i = 1; i < values.size(); i++) {
        outStream.write(Utilities.tabCode);
        outStream.writeBytes((values.get(i) == null ? Utilities.nullStringOutput : values.get(i)));
      }
      outStream.write(Utilities.newLineCode);
    } catch (IOException e) {
      throw new SemanticException(e);
    } finally {
      IOUtils.closeStream(outStream);
    }
  }

  public static Iterable<? extends String> matchesDb(Hive db, String dbPattern) throws HiveException {
    if (dbPattern == null) {
      return db.getAllDatabases();
    } else {
      return db.getDatabasesByPattern(dbPattern);
    }
  }

  public static Iterable<? extends String> matchesTbl(Hive db, String dbName, String tblPattern)
      throws HiveException {
    if (tblPattern == null) {
      return Collections2.filter(db.getAllTables(dbName),
          tableName -> {
            assert tableName != null;
            return !tableName.toLowerCase().startsWith(
                SemanticAnalyzer.VALUES_TMP_TABLE_NAME_PREFIX.toLowerCase());
          });
    } else {
      return db.getTablesByPattern(dbName, tblPattern);
    }
  }
}
