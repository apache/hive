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
package org.apache.hadoop.hive.ql.externalDB;

public class Oracle extends AbstractExternalDB {
  @Override
  public String getRootUser() {
    return "SYS as SYSDBA";
  }

  @Override
  public String getRootPassword() {
    return "oracle";
  }

  @Override
  public String getJdbcUrl() {
    return "jdbc:oracle:thin:@//" + getContainerHostAddress() + ":1521/xe";
  }

  @Override
  public String getJdbcDriver() {
    return "oracle.jdbc.OracleDriver";
  }

  @Override
  protected String getDockerImageName() {
    return "abstractdog/oracle-xe:18.4.0-slim";
  }

  @Override
  protected String[] getDockerAdditionalArgs() {
    return new String[] { "-p", "1521:1521", "-d", "-e", "ORACLE_PASSWORD=" + getRootPassword() };
  }

  @Override
  public boolean isContainerReady(ProcessResults pr) {
    return pr.stdout.contains("DATABASE IS READY TO USE!");
  }

}
