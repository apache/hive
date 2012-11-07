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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;


/** A dummy implementation for
 * {@link org.apache.hadoop.hive.metastore.MetaStoreEndFunctionListener}
 * for testing purposes.
 */
public class DummyEndFunctionListener extends MetaStoreEndFunctionListener{

  public static final List<String> funcNameList = new ArrayList<String>();
  public static final List<MetaStoreEndFunctionContext> contextList =
    new ArrayList<MetaStoreEndFunctionContext>();

  public DummyEndFunctionListener(Configuration config) {
    super(config);
  }

  @Override
  public void onEndFunction(String functionName, MetaStoreEndFunctionContext context) {
    funcNameList.add(functionName);
    contextList.add(context);
  }

}
