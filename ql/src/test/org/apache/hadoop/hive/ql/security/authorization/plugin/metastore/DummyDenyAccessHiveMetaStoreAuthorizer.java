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

package org.apache.hadoop.hive.ql.security.authorization.plugin.metastore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.events.PreEventContext;
import org.apache.hadoop.security.UserGroupInformation;

/*
Dummy HiveMetaStoreAuthorizer to check whether HiveMetaStoreAuthzInfo is getting created by HiveMetaStoreAuthorizer
 */

public class DummyDenyAccessHiveMetaStoreAuthorizer extends HiveMetaStoreAuthorizer {
  public DummyDenyAccessHiveMetaStoreAuthorizer(Configuration config) {
    super(config);
  }
  private String user = null;
  private String err  = null;

  @Override
  HiveMetaStoreAuthzInfo buildAuthzContext (PreEventContext preEventContext) throws MetaException {
    HiveMetaStoreAuthzInfo hiveMetaStoreAuthzInfo = null;
    try {
      user = UserGroupInformation.getLoginUser().getShortUserName();
    } catch (Exception e) {
      err = "Unable to get UserGroupInformation";
    }

    try {
      if (!isSuperUser(user)) {
        hiveMetaStoreAuthzInfo = super.buildAuthzContext(preEventContext);
        err = "Operation type " + preEventContext.getEventType() + " not allowed for user:" + user;
        if (isViewOperation(preEventContext)){
          err = "Operation type " + getViewEventType(preEventContext) + " allowed for user:" + user;
        }
      } else {
        err = "Operation type " + getViewEventType(preEventContext) + " allowed for user:" + user;
      }
    } catch (Exception e) {
      err =  e.getMessage();
    }
    throw  new MetaException(err);
  }

  private String getViewEventType(PreEventContext preEventContext) {
    String ret = preEventContext.getEventType().name();
    PreEventContext.PreEventType preEventType = preEventContext.getEventType();

    switch(preEventType) {
      case CREATE_TABLE:
        ret = "CREATE_VIEW";
        break;
      case ALTER_TABLE:
        ret = "ALTER_VIEW";
        break;
      case DROP_TABLE:
        ret = "DROP_VIEW";
        break;
    }
    return ret;
  }

}
