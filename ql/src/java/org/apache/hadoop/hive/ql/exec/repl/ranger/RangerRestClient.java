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
 * RangerRestClient to connect to Ranger service and export policies.
 */
public interface RangerRestClient {
  RangerExportPolicyList exportRangerPolicies(String sourceRangerEndpoint,
                                              String dbName, String rangerHiveServiceName,
                                              HiveConf hiveConf) throws Exception;

  RangerExportPolicyList importRangerPolicies(RangerExportPolicyList rangerExportPolicyList, String dbName,
                                              String baseUrl,
                                              String rangerHiveServiceName,
                                              HiveConf hiveConf) throws Exception;

  void deleteRangerPolicy(String policyName, String baseUrl, String rangerHiveServiceName,
                          HiveConf hiveConf) throws Exception;

  List<RangerPolicy> removeMultiResourcePolicies(List<RangerPolicy> rangerPolicies);

  List<RangerPolicy> changeDataSet(List<RangerPolicy> rangerPolicies, String sourceDbName,
                                   String targetDbName);

  Path saveRangerPoliciesToFile(RangerExportPolicyList rangerExportPolicyList, Path stagingDirPath,
                                String fileName, HiveConf conf) throws Exception;

  RangerExportPolicyList readRangerPoliciesFromJsonFile(Path filePath,
                                                        HiveConf conf) throws SemanticException;

  boolean checkConnection(String url, HiveConf hiveConf) throws Exception;

  RangerPolicy getDenyPolicyForReplicatedDb(String rangerServiceName, String sourceDb,
                                            String targetDb) throws SemanticException;

}
