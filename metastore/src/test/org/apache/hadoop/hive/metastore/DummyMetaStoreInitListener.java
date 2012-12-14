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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.metastore;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreInitContext;
import org.apache.hadoop.hive.metastore.MetaStoreInitListener;
import org.apache.hadoop.hive.metastore.api.MetaException;

/*
 * An implementation of MetaStoreInitListener to verify onInit is called when
 * HMSHandler is initialized
 */
public class DummyMetaStoreInitListener extends MetaStoreInitListener{

  public static boolean wasCalled = false;
  public DummyMetaStoreInitListener(Configuration config) {
    super(config);
  }

  @Override
  public void onInit(MetaStoreInitContext context) throws MetaException {
    wasCalled = true;
  }
}
