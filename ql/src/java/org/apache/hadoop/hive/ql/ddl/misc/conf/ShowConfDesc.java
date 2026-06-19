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
package org.apache.hadoop.hive.ql.ddl.misc.conf;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.ddl.DDLDesc;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

import java.io.Serializable;

/**
 * DDL task description for SHOW CONF commands.
 */
@Explain(displayName = "Show Configuration", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class ShowConfDesc implements DDLDesc, Serializable {
  private static final long serialVersionUID = 1L;

  public static final String SCHEMA = "default,type,desc#string,string,string";

  private Path resFile;
  private String confName;

  public ShowConfDesc(Path resFile, String confName) {
    this.resFile = resFile;
    this.confName = confName;
  }

  @Explain(displayName = "result file", explainLevels = { Level.EXTENDED })
  public Path getResFile() {
    return resFile;
  }

  @Explain(displayName = "conf name", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getConfName() {
    return confName;
  }
}
