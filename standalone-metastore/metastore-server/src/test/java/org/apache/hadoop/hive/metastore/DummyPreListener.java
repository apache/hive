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
package org.apache.hadoop.hive.metastore;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.events.PreEventContext;

/**
 *
 * DummyPreListener.
 *
 * An implementation of MetaStorePreEventListener which stores the Events it's seen in a list.
 */
public class DummyPreListener extends MetaStorePreEventListener {

  public static final List<PreEventContext> notifyList = new ArrayList<>();

  public DummyPreListener(Configuration config) {
    super(config);
  }

  @Override
  public void onEvent(PreEventContext context) throws MetaException, NoSuchObjectException,
      InvalidOperationException {
    notifyList.add(context);
  }

}
