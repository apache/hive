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

package org.apache.hadoop.hive.ql.ddl.function;

import java.io.Serializable;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.ddl.DDLDesc;
import org.apache.hadoop.hive.ql.ddl.DDLTask2;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

/**
 * DDL task description for SHOW FUNCTIONS commands.
 */
@Explain(displayName = "Show Functions", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class ShowFunctionsDesc implements DDLDesc, Serializable {
  private static final long serialVersionUID = 1L;

  public static final String SCHEMA = "tab_name#string";

  static {
    DDLTask2.registerOperation(ShowFunctionsDesc.class, ShowFunctionsOperation.class);
  }

  private final String resFile;
  private final String pattern;
  private final boolean isLikePattern;

  public ShowFunctionsDesc(Path resFile) {
    this(resFile, null, false);
  }

  public ShowFunctionsDesc(Path resFile, String pattern) {
    this(resFile, pattern, false);
  }

  public ShowFunctionsDesc(Path resFile, String pattern, boolean isLikePattern) {
    this.resFile = resFile.toString();
    this.pattern = pattern;
    this.isLikePattern = isLikePattern;
  }

  @Explain(displayName = "result file", explainLevels = { Level.EXTENDED })
  public String getResFile() {
    return resFile;
  }

  @Explain(displayName = "pattern")
  public String getPattern() {
    return pattern;
  }

  public boolean getIsLikePattern() {
    return isLikePattern;
  }
}
