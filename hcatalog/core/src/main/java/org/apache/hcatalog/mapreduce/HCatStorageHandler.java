/**
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

package org.apache.hcatalog.mapreduce;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.mapred.OutputFormat;

/**
 * The abstract Class HCatStorageHandler would server as the base class for all
 * the storage handlers required for non-native tables in HCatalog.
 * @deprecated Use/modify {@link org.apache.hadoop.hive.ql.metadata.HiveStorageHandler} instead
 */
public abstract class HCatStorageHandler implements HiveStorageHandler {

  //TODO move this to HiveStorageHandler

  /**
   * This method is called to allow the StorageHandlers the chance
   * to populate the JobContext.getConfiguration() with properties that
   * maybe be needed by the handler's bundled artifacts (ie InputFormat, SerDe, etc).
   * Key value pairs passed into jobProperties is guaranteed to be set in the job's
   * configuration object. User's can retrieve "context" information from tableDesc.
   * User's should avoid mutating tableDesc and only make changes in jobProperties.
   * This method is expected to be idempotent such that a job called with the
   * same tableDesc values should return the same key-value pairs in jobProperties.
   * Any external state set by this method should remain the same if this method is
   * called again. It is up to the user to determine how best guarantee this invariant.
   *
   * This method in particular is to create a configuration for input.
   * @param tableDesc
   * @param jobProperties
   */
  public abstract void configureInputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties);

  //TODO move this to HiveStorageHandler

  /**
   * This method is called to allow the StorageHandlers the chance
   * to populate the JobContext.getConfiguration() with properties that
   * maybe be needed by the handler's bundled artifacts (ie InputFormat, SerDe, etc).
   * Key value pairs passed into jobProperties is guaranteed to be set in the job's
   * configuration object. User's can retrieve "context" information from tableDesc.
   * User's should avoid mutating tableDesc and only make changes in jobProperties.
   * This method is expected to be idempotent such that a job called with the
   * same tableDesc values should return the same key-value pairs in jobProperties.
   * Any external state set by this method should remain the same if this method is
   * called again. It is up to the user to determine how best guarantee this invariant.
   *
   * This method in particular is to create a configuration for output.
   * @param tableDesc
   * @param jobProperties
   */
  public abstract void configureOutputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties);

  /**
   *
   *
   * @return authorization provider
   * @throws HiveException
   */
  public abstract HiveAuthorizationProvider getAuthorizationProvider()
    throws HiveException;

  /*
  * (non-Javadoc)
  *
  * @see org.apache.hadoop.hive.ql.metadata.HiveStorageHandler#
  * configureTableJobProperties(org.apache.hadoop.hive.ql.plan.TableDesc,
  * java.util.Map)
  */
  @Override
  @Deprecated
  public final void configureTableJobProperties(TableDesc tableDesc,
                          Map<String, String> jobProperties) {
  }

  /*
  * (non-Javadoc)
  *
  * @see org.apache.hadoop.conf.Configurable#getConf()
  */
  @Override
  public abstract Configuration getConf();

  /*
  * (non-Javadoc)
  *
  * @see org.apache.hadoop.conf.Configurable#setConf(org.apache.hadoop.conf.
  * Configuration)
  */
  @Override
  public abstract void setConf(Configuration conf);

  OutputFormatContainer getOutputFormatContainer(OutputFormat outputFormat) {
    return new DefaultOutputFormatContainer(outputFormat);
  }

}
