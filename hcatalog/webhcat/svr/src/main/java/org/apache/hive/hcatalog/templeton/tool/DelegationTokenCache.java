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
package org.apache.hive.hcatalog.templeton.tool;

import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.common.classification.InterfaceStability;

import java.util.concurrent.ConcurrentHashMap;

/*
 * Cache of delegation tokens.  When {@link TempletonControllerJob} submits a job that requires
 * metastore access and this access should be secure, TCJ will add a delegation token to the
 * submitted job.  When the job completes we need to cancel the token since by default the token
 * lives for 7 days and over time can cause OOM (if not cancelled).  Cancelling from 
 * TempletonControllerJob.LauchMapper mapper (via custom OutputCommitter for example) requires
 * the jar containing HiveMetastoreClient (and any dependent jars) to be available on the node
 * running LaunchMapper.  Specifying transitive closure of the necessary jars is 
 * configuration/maintenance headache for each release.  Caching the token means cancellation is 
 * done from WebHCat server and thus has Hive jars on the classpath.
 * 
 * While it's possible that WebHCat crashes and looses this in-memory state, but this would be an
 * exceptional condition and since tokens will automatically be cancelled after 7 days, 
 * the fact that this info is not persisted is OK.  (Persisting it also complicates things 
 * because that needs to be done securely)
 * @see TempletonControllerJob
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class DelegationTokenCache<JobId, TokenObject> {
  private ConcurrentHashMap<JobId, TokenObject> tokenCache =
          new ConcurrentHashMap<JobId, TokenObject>();
  private static final DelegationTokenCache<String, String> stringFormTokenCache =
          new DelegationTokenCache<String, String>();

  /*
   * Returns the singleton instance of jobId->delegation-token-in-string-form cache
   */
  public static DelegationTokenCache<String, String> getStringFormTokenCache() {
    return stringFormTokenCache;
  }
  TokenObject storeDelegationToken(JobId jobId, TokenObject token) {
    return tokenCache.put(jobId, token);
  }
  public TokenObject getDelegationToken(JobId jobId) {
    return tokenCache.get(jobId);
  }
  public void removeDelegationToken(JobId jobId) {
    tokenCache.remove(jobId);
  }
}
