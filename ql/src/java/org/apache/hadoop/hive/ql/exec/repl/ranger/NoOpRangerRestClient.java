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

package org.apache.hadoop.hive.ql.exec.repl.ranger;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import java.util.List;

/**
 * NoOpRangerRestClient returns empty policies.
 */
public class NoOpRangerRestClient implements RangerRestClient {

  @Override
  public RangerExportPolicyList exportRangerPolicies(String sourceRangerEndpoint,
                                                     String dbName, String rangerHiveServiceName,
                                                     HiveConf hiveConf) {
    return new RangerExportPolicyList();
  }

  @Override
  public RangerExportPolicyList importRangerPolicies(RangerExportPolicyList rangerExportPolicyList, String dbName,
                                                     String baseUrl,
                                                     String rangerHiveServiceName,
                                                     HiveConf hiveConf) throws Exception {
    return null;
  }

  public void deleteRangerPolicy(String policyName, String baseUrl, String rangerHiveServiceName,
                                 HiveConf hiveConf) throws Exception {
    return;
  }

  @Override
  public List<RangerPolicy> removeMultiResourcePolicies(List<RangerPolicy> rangerPolicies) {
    return null;
  }

  @Override
  public List<RangerPolicy> changeDataSet(List<RangerPolicy> rangerPolicies, String sourceDbName,
                                          String targetDbName) {
    return null;
  }

  @Override
  public Path saveRangerPoliciesToFile(RangerExportPolicyList rangerExportPolicyList, Path stagingDirPath,
                                       String fileName, HiveConf conf) throws Exception {
    return null;
  }

  @Override
  public RangerExportPolicyList readRangerPoliciesFromJsonFile(Path filePath, HiveConf conf) throws SemanticException {
    return new RangerExportPolicyList();
  }

  @Override
  public boolean checkConnection(String url, HiveConf hiveConf) throws Exception {
    return true;
  }

  @Override
  public RangerPolicy getDenyPolicyForReplicatedDb(String rangerServiceName,
                                                   String sourceDb, String targetDb) throws SemanticException {
    return null;
  }

}
