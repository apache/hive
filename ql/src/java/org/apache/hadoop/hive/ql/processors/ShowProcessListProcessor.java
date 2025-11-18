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
package org.apache.hadoop.hive.ql.processors;

import com.google.common.base.Joiner;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.ql.session.ProcessListInfo;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_NULL_FORMAT;
import static org.apache.hadoop.hive.serde.serdeConstants.STRING_TYPE_NAME;
import static org.apache.hadoop.hive.serde2.MetadataTypedColumnsetSerDe.defaultNullString;

/**
 * List operations/queries being performed in sessions within hiveserver2
 */
public class ShowProcessListProcessor implements CommandProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(ShowProcessListProcessor.class.getName());
  private static final SessionState.LogHelper console = new SessionState.LogHelper(LOG);
  private List<ProcessListInfo> liveQueries = null;

  public void setup(List<ProcessListInfo> liveQueries) {
    this.liveQueries = liveQueries;
  }

  /**
   * Creates a Schema object with running operation details
   *
   * @return
   */
  private Schema getSchema() {
    Schema sch = new Schema();
    sch.addToFieldSchemas(new FieldSchema("User Name", STRING_TYPE_NAME, ""));
    sch.addToFieldSchemas(new FieldSchema("Ip Addr", STRING_TYPE_NAME, ""));
    sch.addToFieldSchemas(new FieldSchema("Execution Engine", STRING_TYPE_NAME, ""));
    sch.addToFieldSchemas(new FieldSchema("Session Id", STRING_TYPE_NAME, ""));
    sch.addToFieldSchemas(new FieldSchema("Session Active Time (s)", STRING_TYPE_NAME, ""));
    sch.addToFieldSchemas(new FieldSchema("Session Idle Time (s)", STRING_TYPE_NAME, ""));
    sch.addToFieldSchemas(new FieldSchema("Query ID", STRING_TYPE_NAME, ""));
    sch.addToFieldSchemas(new FieldSchema("State", STRING_TYPE_NAME, ""));
    sch.addToFieldSchemas(new FieldSchema("Txn ID", STRING_TYPE_NAME, ""));
    sch.addToFieldSchemas(new FieldSchema("Opened Timestamp (s)", STRING_TYPE_NAME, ""));
    sch.addToFieldSchemas(new FieldSchema("Elapsed Time (s)", STRING_TYPE_NAME, ""));
    sch.addToFieldSchemas(new FieldSchema("Runtime (s)", STRING_TYPE_NAME, ""));
    sch.putToProperties(SERIALIZATION_NULL_FORMAT, defaultNullString);
    return sch;
  }

  @Override
  public CommandProcessorResponse run(String command) throws CommandProcessorException {
    try {
      String[] tokens = command.split("\\s+");
      boolean isCorrectSubCommand = HiveCommand.PROCESSLIST.name().equalsIgnoreCase(tokens[0]);

      if (tokens.length != 1 || !isCorrectSubCommand) {
        throw new CommandProcessorException("Show ProcessList Failed: Unsupported sub-command option");
      }
      // TODO : Authorization?
      if (CollectionUtils.isEmpty(liveQueries)) {
        return new CommandProcessorResponse(getSchema(), "No queries running currently");
      }
      SessionState ss = SessionState.get();
      liveQueries.forEach(query -> {
            ss.out.println(
                Joiner.on("\t").join(
                    query.getUserName(),
                    query.getIpAddr(),
                    query.getExecutionEngine(),
                    query.getSessionId(),
                    query.getSessionActiveTime(),
                    query.getSessionIdleTime(),
                    query.getQueryId(),
                    query.getState(),
                    query.getTxnId(),
                    query.getBeginTime(),
                    query.getElapsedTime(),
                    query.getRuntime()
                ));
          }
      );
      return new CommandProcessorResponse(getSchema(), null);
    } catch (Exception e) {
      console.printError("Exception raised from ShowProcessListProcessor.run "
          + e.getLocalizedMessage(), org.apache.hadoop.util.StringUtils
          .stringifyException(e));
      throw new CommandProcessorException(e.getLocalizedMessage(), e);
    }
  }

  /**
   * There are no resources to be closed ,hence this method is empty.
   *
   * @throws Exception
   */
  @Override
  public void close() throws Exception {
  }

}
