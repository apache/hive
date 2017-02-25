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
package org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd;

import java.util.ArrayList;
import java.util.List;
import java.util.HashSet;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAccessControlException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzPluginException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzSessionContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveMetastoreClientFactory;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * Extends SQLStdHiveAuthorizationValidator to relax the restriction of not
 * being able to run dfs,set commands. To be used for testing purposes only!
 *
 * In addition, it parses a setting test.hive.authz.sstd.validator.bypassObjTypes
 * as a comma-separated list of object types, which, if present, it will bypass
 * validations of all input and output objects of those types.
 */

@Private
public class SQLStdHiveAuthorizationValidatorForTest extends SQLStdHiveAuthorizationValidator {

  final String BYPASS_OBJTYPES_KEY = "test.hive.authz.sstd.validator.bypassObjTypes";
  Set<HivePrivilegeObject.HivePrivilegeObjectType> bypassObjectTypes;

  public SQLStdHiveAuthorizationValidatorForTest(HiveMetastoreClientFactory metastoreClientFactory,
      HiveConf conf, HiveAuthenticationProvider authenticator,
      SQLStdHiveAccessControllerWrapper privController, HiveAuthzSessionContext ctx)
      throws HiveAuthzPluginException {
    super(metastoreClientFactory, conf, authenticator, privController, ctx);
    setupBypass(conf.get(BYPASS_OBJTYPES_KEY,""));
  }

  private void setupBypass(String bypassObjectTypesConf){
    bypassObjectTypes = new HashSet<HivePrivilegeObject.HivePrivilegeObjectType>();
    if (!bypassObjectTypesConf.isEmpty()){
      for (String bypassType : bypassObjectTypesConf.split(",")){
        if ((bypassType != null) && !bypassType.isEmpty()){
          bypassObjectTypes.add(HivePrivilegeObject.HivePrivilegeObjectType.valueOf(bypassType));
        }
      }
    }
  }

  List<HivePrivilegeObject> filterForBypass(List<HivePrivilegeObject> privilegeObjects){
    if (privilegeObjects == null){
      return null;
    } else {
      return Lists.newArrayList(Iterables.filter(privilegeObjects,new Predicate<HivePrivilegeObject>() {
        @Override
        public boolean apply(@Nullable HivePrivilegeObject hivePrivilegeObject) {
          // Return true to retain an item, and false to filter it out.
          if (hivePrivilegeObject == null){
            return true;
          }
          return !bypassObjectTypes.contains(hivePrivilegeObject.getType());
        }
      }));
    }
  }

  @Override
  public void checkPrivileges(HiveOperationType hiveOpType, List<HivePrivilegeObject> inputHObjs,
      List<HivePrivilegeObject> outputHObjs, HiveAuthzContext context) throws HiveAuthzPluginException,
      HiveAccessControlException {
    switch (hiveOpType) {
    case DFS:
    case SET:
      // allow SET and DFS commands to be used during testing
      return;
    default:
      super.checkPrivileges(hiveOpType, filterForBypass(inputHObjs), filterForBypass(outputHObjs), context);
    }

  }

  public boolean needTransform() {
    // In the future, we can add checking for username, groupname, etc based on
    // HiveAuthenticationProvider. For example,
    // "hive_test_user".equals(context.getUserName());
    return true;
  }

  // Please take a look at the instructions in HiveAuthorizer.java before
  // implementing applyRowFilterAndColumnMasking
  public List<HivePrivilegeObject> applyRowFilterAndColumnMasking(HiveAuthzContext context,
      List<HivePrivilegeObject> privObjs) throws SemanticException {
    List<HivePrivilegeObject> needRewritePrivObjs = new ArrayList<>(); 
    for (HivePrivilegeObject privObj : privObjs) {
      if (privObj.getObjectName().equals("masking_test")) {
        privObj.setRowFilterExpression("key % 2 = 0 and key < 10");
        List<String> cellValueTransformers = new ArrayList<>();
        for (String columnName : privObj.getColumns()) {
          if (columnName.equals("value")) {
            cellValueTransformers.add("reverse(value)");
          } else {
            cellValueTransformers.add(columnName);
          }
        }
        privObj.setCellValueTransformers(cellValueTransformers);
        needRewritePrivObjs.add(privObj);
      } else if (privObj.getObjectName().equals("masking_test_subq")) {
        privObj
            .setRowFilterExpression("key in (select key from src where src.key = masking_test_subq.key)");
        needRewritePrivObjs.add(privObj);
      } else if (privObj.getObjectName().equals("masking_acid_no_masking")) {
        // testing acid usage when no masking/filtering is present
        needRewritePrivObjs.add(privObj);
      }
    }
    return needRewritePrivObjs;
  }

}
