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

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.session.SessionState;

/**
 * used for reload auxiliary and jars without restarting hive server2
 */
public class ReloadProcessor implements CommandProcessor{
  private static final Logger LOG = LoggerFactory.getLogger(ReloadProcessor.class);

  @Override
  public CommandProcessorResponse run(String command) {
    SessionState ss = SessionState.get();
    try {
      ss.loadReloadableAuxJars();
    } catch (IOException e) {
      LOG.error("fail to reload auxiliary jar files", e);
      return CommandProcessorResponse.create(e);
    }
    return new CommandProcessorResponse(0);
  }

  @Override
  public void close() throws Exception {
  }
}
