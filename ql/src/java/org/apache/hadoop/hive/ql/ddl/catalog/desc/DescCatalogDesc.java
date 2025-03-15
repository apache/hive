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

package org.apache.hadoop.hive.ql.ddl.catalog.desc;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.ddl.DDLDesc;
import org.apache.hadoop.hive.ql.plan.Explain;

import java.io.Serializable;

/**
 * DDL task description for DESC CATALOG commands.
 */
@Explain(displayName = "Describe Catalog", explainLevels = { Explain.Level.USER, Explain.Level.DEFAULT, Explain.Level.EXTENDED })
public class DescCatalogDesc implements DDLDesc, Serializable {
  private static final long serialVersionUID = 1L;

  public static final String DESC_CATALOG_SCHEMA = "cat_name,comment,location#string:string:string";

  public static final String DESC_CATALOG_SCHEMA_EXTENDED = "cat_name,comment,location,create_time#string:string:string:string";

  private final String resFile;
  private final String catName;
  private final boolean isExtended;

  public DescCatalogDesc(Path resFile, String catName, boolean isExtended) {
    this.resFile = resFile.toString();
    this.catName = catName;
    this.isExtended = isExtended;
  }

  @Explain(displayName = "result file", explainLevels = { Explain.Level.EXTENDED })
  public String getResFile() {
    return resFile;
  }

  @Explain(displayName = "catalog", explainLevels = { Explain.Level.USER, Explain.Level.DEFAULT, Explain.Level.EXTENDED })
  public String getCatName() {
    return catName;
  }

  @Explain(displayName = "extended", displayOnlyOnTrue=true,
      explainLevels = { Explain.Level.USER, Explain.Level.DEFAULT, Explain.Level.EXTENDED })
  public boolean isExtended() {
    return isExtended;
  }

  public String getSchema() {
    return isExtended ? DESC_CATALOG_SCHEMA_EXTENDED : DESC_CATALOG_SCHEMA;
  }
}
