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

package org.apache.hcatalog.mapreduce;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Job;

/** The InputFormat to use to read data from HCatalog. */
public class HCatInputFormat extends HCatBaseInputFormat {

  /**
   * Set the input information to use for the job. This queries the metadata server 
   * with the specified partition predicates, gets the matching partitions, and 
   * puts the information in the conf object. The inputInfo object is updated 
   * with information needed in the client context.
   * @param job the job object
   * @param inputJobInfo the input information about the table to read
   * @throws IOException the exception in communicating with the metadata server
   */
  public static void setInput(Job job,
      InputJobInfo inputJobInfo) throws IOException {
    try {
      InitializeInput.setInput(job, inputJobInfo);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }


}
