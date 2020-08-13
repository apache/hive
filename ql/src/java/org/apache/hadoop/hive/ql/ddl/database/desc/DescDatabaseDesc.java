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

package org.apache.hadoop.hive.ql.ddl.database.desc;

import java.io.Serializable;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.ddl.DDLDesc;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

/**
 * DDL task description for DESC DATABASE commands.
 */
@Explain(displayName = "Describe Database", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class DescDatabaseDesc implements DDLDesc, Serializable {
  private static final long serialVersionUID = 1L;

  public static final String DESC_DATABASE_SCHEMA =
      "db_name,comment,location,managedLocation,owner_name,owner_type,connector_name,remote_dbname#string:string:string:string:string:string,string,string";

  public static final String DESC_DATABASE_SCHEMA_EXTENDED =
      "db_name,comment,location,managedLocation,owner_name,owner_type,connector_name,remote_dbname,parameters#" +
      "string:string:string:string:string:string:string,string,string";

  private final String resFile;
  private final String dbName;
  private final boolean isExtended;

  public DescDatabaseDesc(Path resFile, String dbName, boolean isExtended) {
    this.resFile = resFile.toString();
    this.dbName = dbName;
    this.isExtended = isExtended;
  }

  @Explain(displayName = "extended", displayOnlyOnTrue=true,
      explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public boolean isExtended() {
    return isExtended;
  }

  @Explain(displayName = "database", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getDatabaseName() {
    return dbName;
  }

  @Explain(displayName = "result file", explainLevels = { Level.EXTENDED })
  public String getResFile() {
    return resFile;
  }

  public String getSchema() {
    return isExtended ? DESC_DATABASE_SCHEMA_EXTENDED : DESC_DATABASE_SCHEMA;
  }
}
