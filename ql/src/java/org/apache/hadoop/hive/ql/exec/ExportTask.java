/*
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
 */
package org.apache.hadoop.hive.ql.exec;

import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.repl.dump.TableExport;
import org.apache.hadoop.hive.ql.plan.ExportWork;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

public class ExportTask extends Task<ExportWork> implements Serializable {

  private static final long serialVersionUID = 1L;
  private Logger LOG = LoggerFactory.getLogger(ExportTask.class);

  public ExportTask() {
    super();
  }

  @Override
  public String getName() {
    return "EXPORT";
  }

  @Override
  protected int execute(DriverContext driverContext) {
    try {
      // Also creates the root directory
      TableExport.Paths exportPaths =
          new TableExport.Paths(work.getAstRepresentationForErrorMsg(), work.getExportRootDir(),
              conf, false);
      Hive db = getHive();
      LOG.debug("Exporting data to: {}", exportPaths.getExportRootDir());
      work.acidPostProcess(db);
      TableExport tableExport = new TableExport(
          exportPaths, work.getTableSpec(), work.getReplicationSpec(), db, null, conf
      );
      if (!tableExport.write()) {
        throw new SemanticException(ErrorMsg.EXIM_FOR_NON_NATIVE.getMsg());
      }
    } catch (Exception e) {
      LOG.error("failed", e);
      setException(e);
      return 1;
    }
    return 0;
  }

  @Override
  public StageType getType() {
    // TODO: Modify Thrift IDL to generate export stage if needed
    return StageType.REPL_DUMP;
  }
}
