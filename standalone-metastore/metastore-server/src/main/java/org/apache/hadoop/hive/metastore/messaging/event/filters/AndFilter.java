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
package org.apache.hadoop.hive.metastore.messaging.event.filters;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;

public class AndFilter implements IMetaStoreClient.NotificationFilter {
  final IMetaStoreClient.NotificationFilter[] filters;

  public AndFilter(final IMetaStoreClient.NotificationFilter... filters) {
    this.filters = filters;
  }

  @Override
  public boolean accept(final NotificationEvent event) {
    for (IMetaStoreClient.NotificationFilter filter : filters) {
      if (!filter.accept(event)) {
        return false;
      }
    }
    return true;
  }
}
