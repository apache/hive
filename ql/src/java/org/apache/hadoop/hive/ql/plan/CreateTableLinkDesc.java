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

package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;
import java.util.Map;

/**
 * CreateTableLinkDesc.
 *
 */
@Explain(displayName = "Create TableLink")
public class CreateTableLinkDesc extends DDLDesc implements Serializable {

  private static final long serialVersionUID = 1L;
  String targetTable;
  String targetDatabase;
  boolean isStatic;
  Map<String, String> linkProps;

  public CreateTableLinkDesc(String targetTable, String targetDatabase,
      boolean isStatic, Map<String, String> linkProps) {
    this.targetTable = targetTable;
    this.targetDatabase = targetDatabase;
    this.isStatic = isStatic;
    this.linkProps = linkProps;
  }

  @Explain(displayName = "target table")
  public String getTargetTable() {
    return targetTable;
  }

  @Explain(displayName = "target database" )
  public String getTargetDatabase() {
    return targetDatabase;
  }

  @Explain(displayName = "is static link")
  public boolean isStaticLink() {
    return isStatic;
  }

  @Explain(displayName = "link properties")
  public Map<String, String> getLinkProps() {
    return linkProps;
  }
}
