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
import org.apache.hadoop.io.BooleanWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * UDF to determine whether to enforce restriction of information schema.
 * This is intended for internal usage only. This function is not a deterministic function,
 * but a runtime constant. The return value is constant within a query but can be different between queries
 */
@UDFType(deterministic = false, runtimeConstant = true)
@Description(name = "restrict_information_schema",
    value = "_FUNC_() - Returns whether or not to enable information schema restriction. " +
    "Currently it is enabled if either HS2 authorizer or metastore authorizer implements policy provider " +
    "interface.")
@NDV(maxNdv = 1)
public class GenericUDFRestrictInformationSchema extends GenericUDF {
  private static final Logger LOG = LoggerFactory.getLogger(GenericUDFRestrictInformationSchema.class.getName());
  protected BooleanWritable enabled;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 0) {
      throw new UDFArgumentLengthException(
          "The function RestrictInformationSchema does not take any arguments, but found " + arguments.length);
    }

    if (enabled == null) {
      HiveConf hiveConf = SessionState.getSessionConf();

      boolean enableHS2PolicyProvider = false;
      boolean enableMetastorePolicyProvider = false;

      HiveAuthorizer authorizer = SessionState.get().getAuthorizerV2();
      try {
        if (hiveConf.getBoolVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_ENABLED)
            && authorizer.getHivePolicyProvider() != null) {
          enableHS2PolicyProvider = true;
        }
      } catch (HiveAuthzPluginException e) {
        LOG.warn("Error getting HivePolicyProvider", e);
      }

      if (!enableHS2PolicyProvider) {
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
                enableMetastorePolicyProvider = true;
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
      if (enableHS2PolicyProvider || enableMetastorePolicyProvider) {
        enabled = new BooleanWritable(true);
      } else {
        enabled = new BooleanWritable(false);
      }
    }

    return PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    return enabled;
  }

  @Override
  public String getDisplayString(String[] children) {
    return "RESTRICT_INFORMATION_SCHEMA()";
  }

  @Override
  public void copyToNewInstance(Object newInstance) throws UDFArgumentException {
    super.copyToNewInstance(newInstance);
    // Need to preserve enabled flag
    GenericUDFRestrictInformationSchema other = (GenericUDFRestrictInformationSchema) newInstance;
    if (this.enabled != null) {
      other.enabled = new BooleanWritable(this.enabled.get());
    }
  }
}
