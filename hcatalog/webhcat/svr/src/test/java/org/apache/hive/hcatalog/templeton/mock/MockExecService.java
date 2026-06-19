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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.hcatalog.templeton.mock;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.exec.ExecuteException;
import org.apache.hive.hcatalog.templeton.ExecBean;
import org.apache.hive.hcatalog.templeton.ExecService;
import org.apache.hive.hcatalog.templeton.NotAuthorizedException;

public class MockExecService implements ExecService {

  public ExecBean run(String program, List<String> args,
            Map<String, String> env) {
    ExecBean bean = new ExecBean();
    bean.stdout = program;
    bean.stderr = args.toString();
    return bean;
  }

  @Override
  public ExecBean runUnlimited(String program,
                 List<String> args, Map<String, String> env)
    throws NotAuthorizedException, ExecuteException, IOException {
    ExecBean bean = new ExecBean();
    bean.stdout = program;
    bean.stderr = args.toString();
    return null;
  }
}
