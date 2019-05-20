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

package org.apache.hadoop.hive.ql.ddl.table.partition;

import java.io.Serializable;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.ddl.DDLDesc;
import org.apache.hadoop.hive.ql.ddl.DDLTask2;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

/**
 * DDL task description for SHOW PARTITIONS commands.
 */
@Explain(displayName = "Show Partitions", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class ShowPartitionsDesc implements DDLDesc, Serializable {
  private static final long serialVersionUID = 1L;

  static {
    DDLTask2.registerOperation(ShowPartitionsDesc.class, ShowPartitionsOperation.class);
  }

  public static final String SCHEMA = "partition#string";

  private final String tabName;
  private final String resFile;
  private final Map<String, String> partSpec;

  public ShowPartitionsDesc(String tabName, Path resFile, Map<String, String> partSpec) {
    this.tabName = tabName;
    this.resFile = resFile.toString();
    this.partSpec = partSpec;
  }

  @Explain(displayName = "table", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getTabName() {
    return tabName;
  }

  @Explain(displayName = "partSpec", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public Map<String, String> getPartSpec() {
    return partSpec;
  }

  @Explain(displayName = "result file", explainLevels = { Level.EXTENDED })
  public String getResFile() {
    return resFile;
  }
}
