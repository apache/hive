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

package org.apache.hadoop.hive.ql.udf.generic;

import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.security.authorization.HiveMetastoreAuthorizationProvider;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizer;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzPluginException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * UDF to determine the current authorizer (class name of the authorizer)
 * This is intended for internal usage only. This function is not a deterministic function,
 * but a runtime constant. The return value is constant within a query but can be different between queries
 */
@UDFType(deterministic = false, runtimeConstant = true)
@Description(name = "current_authorizer",
    value = "_FUNC_() - Returns the current authorizer (class name of the authorizer). ")
@NDV(maxNdv = 1)
public class GenericUDFCurrentAuthorizer extends GenericUDF {
  private static final Logger LOG = LoggerFactory.getLogger(GenericUDFCurrentAuthorizer.class.getName());
  protected Text authorizer;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 0) {
      throw new UDFArgumentLengthException(
          "The function CurrentAuthorizer does not take any arguments, but found " + arguments.length);
    }

    if (authorizer == null) {

      HiveConf hiveConf = SessionState.getSessionConf();
      HiveAuthorizer hiveAuthorizer = SessionState.get().getAuthorizerV2();
      try {
        if (hiveAuthorizer.getHivePolicyProvider() != null) {
          authorizer = new Text(hiveAuthorizer.getHivePolicyProvider().getClass().getSimpleName());
        }
      } catch (HiveAuthzPluginException e) {
        LOG.warn("Error getting HivePolicyProvider", e);
      }

      if (authorizer == null) {
        // If authorizer is not set, check for metastore authorizer (eg. StorageBasedAuthorizationProvider)
        if (MetastoreConf.getVar(hiveConf, MetastoreConf.ConfVars.PRE_EVENT_LISTENERS) != null &&
            !MetastoreConf.getVar(hiveConf, MetastoreConf.ConfVars.PRE_EVENT_LISTENERS).isEmpty() &&
            HiveConf.getVar(hiveConf, HiveConf.ConfVars.HIVE_METASTORE_AUTHORIZATION_MANAGER) != null) {
          List<HiveMetastoreAuthorizationProvider> authorizerProviders;
          try {
            authorizerProviders = HiveUtils.getMetaStoreAuthorizeProviderManagers(
              hiveConf, HiveConf.ConfVars.HIVE_METASTORE_AUTHORIZATION_MANAGER,
              SessionState.get().getAuthenticator());
            for (HiveMetastoreAuthorizationProvider authProvider : authorizerProviders) {
              if (authProvider.getHivePolicyProvider() != null) {
                authorizer = new Text(authProvider.getHivePolicyProvider().getClass().getSimpleName());
                break;
              }
            }
          } catch (HiveAuthzPluginException e) {
            LOG.warn("Error getting HivePolicyProvider", e);
          } catch (HiveException e) {
            LOG.warn("Error instantiating hive.security.metastore.authorization.manager", e);
          }
        }
      }
    }

    return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    return authorizer;
  }

  @Override
  public String getDisplayString(String[] children) {
    return "CURRENT_AUTHORIZER()";
  }

  @Override
  public void copyToNewInstance(Object newInstance) throws UDFArgumentException {
    super.copyToNewInstance(newInstance);
    // Need to preserve authorizer flag
    GenericUDFCurrentAuthorizer other = (GenericUDFCurrentAuthorizer) newInstance;
    if (this.authorizer != null) {
      other.authorizer = new Text(this.authorizer);
    }
  }
}
