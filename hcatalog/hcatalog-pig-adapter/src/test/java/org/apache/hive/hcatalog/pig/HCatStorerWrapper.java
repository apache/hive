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

package org.apache.hive.hcatalog.pig;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hive.hcatalog.common.HCatConstants;
import org.apache.pig.impl.util.UDFContext;

/**
 * This class is used to test the HCAT_PIG_STORER_EXTERNAL_LOCATION property used in HCatStorer.
 * When this property is set, HCatStorer writes the output to the location it specifies. Since
 * the property can only be set in the UDFContext, we need this simpler wrapper to do three things:
 * <ol>
 * <li> save the external dir specified in the Pig script </li>
 * <li> set the same UDFContext signature as HCatStorer </li>
 * <li> before {@link HCatStorer#setStoreLocation(String, Job)}, set the external dir in the UDFContext.</li>
 * </ol>
 */
public class HCatStorerWrapper extends HCatStorer {

  private String sign;
  private String externalDir;

  public HCatStorerWrapper(String partSpecs, String schema, String externalDir) throws Exception {
    super(partSpecs, schema);
    this.externalDir = externalDir;
  }

  public HCatStorerWrapper(String partSpecs, String externalDir) throws Exception {
    super(partSpecs);
    this.externalDir = externalDir;
  }

  public HCatStorerWrapper(String externalDir) throws Exception{
    super();
    this.externalDir = externalDir;
  }

  @Override
  public void setStoreLocation(String location, Job job) throws IOException {
    Properties udfProps = UDFContext.getUDFContext().getUDFProperties(
        this.getClass(), new String[] { sign });
    udfProps.setProperty(HCatConstants.HCAT_PIG_STORER_EXTERNAL_LOCATION, externalDir);
    super.setStoreLocation(location, job);
  }

  @Override
  public void setStoreFuncUDFContextSignature(String signature) {
    sign = signature;
    super.setStoreFuncUDFContextSignature(signature);
  }
}
