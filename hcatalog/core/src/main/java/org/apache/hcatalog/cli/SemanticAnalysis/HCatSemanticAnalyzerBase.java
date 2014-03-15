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

package org.apache.hcatalog.cli.SemanticAnalysis;

import java.io.Serializable;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.metadata.AuthorizationException;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.InvalidTableException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.AbstractSemanticAnalyzerHook;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHookContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.DDLWork;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.ql.security.authorization.Privilege;
import org.apache.hadoop.hive.ql.session.SessionState;

/**
 * Base class for HCatSemanticAnalyzer hooks.
 * @deprecated Use/modify {@link org.apache.hive.hcatalog.cli.SemanticAnalysis.HCatSemanticAnalyzerBase} instead
 */
public class HCatSemanticAnalyzerBase extends AbstractSemanticAnalyzerHook {

  private HiveAuthorizationProvider authProvider;

  public HiveAuthorizationProvider getAuthProvider() {
    if (authProvider == null) {
      authProvider = SessionState.get().getAuthorizer();
    }

    return authProvider;
  }

  @Override
  public void postAnalyze(HiveSemanticAnalyzerHookContext context,
              List<Task<? extends Serializable>> rootTasks) throws SemanticException {
    super.postAnalyze(context, rootTasks);

    //Authorize the operation.
    authorizeDDL(context, rootTasks);
  }

  /**
   * Checks for the given rootTasks, and calls authorizeDDLWork() for each DDLWork to
   * be authorized. The hooks should override this, or authorizeDDLWork to perform the
   * actual authorization.
   */
  /*
  * Impl note: Hive provides authorization with it's own model, and calls the defined
  * HiveAuthorizationProvider from Driver.doAuthorization(). However, HCat has to
  * do additional calls to the auth provider to implement expected behavior for
  * StorageDelegationAuthorizationProvider. This means, that the defined auth provider
  * is called by both Hive and HCat. The following are missing from Hive's implementation,
  * and when they are fixed in Hive, we can remove the HCat-specific auth checks.
  * 1. CREATE DATABASE/TABLE, ADD PARTITION statements does not call
  * HiveAuthorizationProvider.authorize() with the candidate objects, which means that
  * we cannot do checks against defined LOCATION.
  * 2. HiveOperation does not define sufficient Privileges for most of the operations,
  * especially database operations.
  * 3. For some of the operations, Hive SemanticAnalyzer does not add the changed
  * object as a WriteEntity or ReadEntity.
  *
  * @see https://issues.apache.org/jira/browse/HCATALOG-244
  * @see https://issues.apache.org/jira/browse/HCATALOG-245
  */
  protected void authorizeDDL(HiveSemanticAnalyzerHookContext context,
                List<Task<? extends Serializable>> rootTasks) throws SemanticException {

    if (!HiveConf.getBoolVar(context.getConf(),
      HiveConf.ConfVars.HIVE_AUTHORIZATION_ENABLED)) {
      return;
    }

    Hive hive;
    try {
      hive = context.getHive();

      for (Task<? extends Serializable> task : rootTasks) {
        if (task.getWork() instanceof DDLWork) {
          DDLWork work = (DDLWork) task.getWork();
          if (work != null) {
            authorizeDDLWork(context, hive, work);
          }
        }
      }
    } catch (SemanticException ex) {
      throw ex;
    } catch (AuthorizationException ex) {
      throw ex;
    } catch (Exception ex) {
      throw new SemanticException(ex);
    }
  }

  /**
   * Authorized the given DDLWork. Does nothing by default. Override this
   * and delegate to the relevant method in HiveAuthorizationProvider obtained by
   * getAuthProvider().
   */
  protected void authorizeDDLWork(HiveSemanticAnalyzerHookContext context,
                  Hive hive, DDLWork work) throws HiveException {
  }

  protected void authorize(Privilege[] inputPrivs, Privilege[] outputPrivs)
    throws AuthorizationException, SemanticException {
    try {
      getAuthProvider().authorize(inputPrivs, outputPrivs);
    } catch (HiveException ex) {
      throw new SemanticException(ex);
    }
  }

  protected void authorize(Database db, Privilege priv)
    throws AuthorizationException, SemanticException {
    try {
      getAuthProvider().authorize(db, null, new Privilege[]{priv});
    } catch (HiveException ex) {
      throw new SemanticException(ex);
    }
  }

  protected void authorizeTable(Hive hive, String tableName, Privilege priv)
    throws AuthorizationException, HiveException {
    Table table;
    try {
      table = hive.getTable(tableName);
    } catch (InvalidTableException ite) {
      // Table itself doesn't exist in metastore, nothing to validate.
      return;
    }

    authorize(table, priv);
  }

  protected void authorize(Table table, Privilege priv)
    throws AuthorizationException, SemanticException {
    try {
      getAuthProvider().authorize(table, new Privilege[]{priv}, null);
    } catch (HiveException ex) {
      throw new SemanticException(ex);
    }
  }

  protected void authorize(Partition part, Privilege priv)
    throws AuthorizationException, SemanticException {
    try {
      getAuthProvider().authorize(part, new Privilege[]{priv}, null);
    } catch (HiveException ex) {
      throw new SemanticException(ex);
    }
  }
}
