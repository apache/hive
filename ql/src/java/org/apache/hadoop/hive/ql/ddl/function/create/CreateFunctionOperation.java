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

package org.apache.hadoop.hive.ql.ddl.function.create;

import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.FunctionUtils;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.FunctionInfo.FunctionResource;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.FunctionType;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.ResourceUri;
import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.util.ResourceDownloader;

/**
 * Operation process of creating a function.
 */
public class CreateFunctionOperation extends DDLOperation<CreateFunctionDesc> {
  public CreateFunctionOperation(DDLOperationContext context, CreateFunctionDesc desc) {
    super(context, desc);
  }

  @Override
  public int execute() throws HiveException {
    if (desc.isTemp()) {
      return createTemporaryFunction();
    } else {
      try {
        return createPermanentFunction();
      } catch (Exception e) {
        return handlePermanentFunctionCreationException(e);
      }
    }
  }

  private int createTemporaryFunction() {
    try {
      // Add any required resources
      FunctionResource[] resources = FunctionUtils.toFunctionResource(desc.getResources());
      FunctionUtils.addFunctionResources(resources);

      Class<?> udfClass = getUdfClass();
      FunctionInfo registered = FunctionRegistry.registerTemporaryUDF(desc.getName(), udfClass, resources);
      if (registered != null) {
        return 0;
      } else {
        context.getConsole().printError(
            "FAILED: Class " + desc.getClassName() + " does not implement UDF, GenericUDF, or UDAF");
        return 1;
      }
    } catch (HiveException e) {
      context.getConsole().printError("FAILED: " + e.toString());
      LOG.info("create function: ", e);
      return 1;
    } catch (ClassNotFoundException e) {
      context.getConsole().printError("FAILED: Class " + desc.getClassName() + " not found");
      LOG.info("create function: ", e);
      return 1;
    }
  }

  private Class<?> getUdfClass() throws ClassNotFoundException {
    // get the session specified class loader from SessionState
    ClassLoader classLoader = Utilities.getSessionSpecifiedClassLoader();
    return Class.forName(desc.getClassName(), true, classLoader);
  }

  // todo authorization
  private int createPermanentFunction() throws HiveException, IOException {
    String[] qualifiedNameParts = FunctionUtils.getQualifiedFunctionNameParts(desc.getName());
    String dbName = qualifiedNameParts[0];
    String functionName = qualifiedNameParts[1];

    if (skipIfNewerThenUpdate(dbName, functionName)) {
      return 0;
    }

    // For permanent functions, check for any resources from local filesystem.
    checkLocalFunctionResources();

    String registeredName = FunctionUtils.qualifyFunctionName(functionName, dbName);
    boolean registrationSuccess = registerFunction(registeredName);
    if (!registrationSuccess) {
      context.getConsole().printError("Failed to register " + registeredName + " using class " + desc.getClassName());
      return 1;
    }

    boolean addToMetastoreSuccess = addToMetastore(dbName, functionName, registeredName);
    if (!addToMetastoreSuccess) {
      return 1;
    }

    return 0;
  }

  private boolean skipIfNewerThenUpdate(String dbName, String functionName) throws HiveException {
    if (desc.getReplicationSpec().isInReplicationScope()) {
      Map<String, String> dbProps = Hive.get().getDatabase(dbName).getParameters();
      if (!desc.getReplicationSpec().allowEventReplacementInto(dbProps)) {
        // If the database is newer than the create event, then noop it.
        LOG.debug("FunctionTask: Create Function {} is skipped as database {} is newer than update",
            functionName, dbName);
        return true;
      }
    }

    return false;
  }

  private void checkLocalFunctionResources() throws HiveException {
    // If this is a non-local warehouse, then adding resources from the local filesystem
    // may mean that other clients will not be able to access the resources.
    // So disallow resources from local filesystem in this case.
    if (CollectionUtils.isNotEmpty(desc.getResources())) {
      try {
        String localFsScheme = FileSystem.getLocal(context.getDb().getConf()).getUri().getScheme();
        String configuredFsScheme = FileSystem.get(context.getDb().getConf()).getUri().getScheme();
        if (configuredFsScheme.equals(localFsScheme)) {
          // Configured warehouse FS is local, don't need to bother checking.
          return;
        }

        for (ResourceUri res : desc.getResources()) {
          String resUri = res.getUri();
          if (ResourceDownloader.isFileUri(resUri)) {
            throw new HiveException("Hive warehouse is non-local, but " + res.getUri() + " specifies file on local "
                + "filesystem. Resources on non-local warehouse should specify a non-local scheme/path");
          }
        }
      } catch (HiveException e) {
        throw e;
      } catch (Exception e) {
        LOG.error("Exception caught in checkLocalFunctionResources", e);
        throw new HiveException(e);
      }
    }
  }

  private boolean registerFunction(String registeredName) throws SemanticException, HiveException {
    FunctionInfo registered = null;
    HiveConf oldConf = SessionState.get().getConf();
    try {
      SessionState.get().setConf(context.getConf());
      registered = FunctionRegistry.registerPermanentFunction(registeredName, desc.getClassName(), true,
          FunctionUtils.toFunctionResource(desc.getResources()));
    } catch (RuntimeException ex) {
      Throwable t = ex;
      while (t.getCause() != null) {
        t = t.getCause();
      }
      context.getTask().setException(t);
    } finally {
      SessionState.get().setConf(oldConf);
    }
    return registered != null;
  }

  private boolean addToMetastore(String dbName, String functionName, String registeredName) throws HiveException {
    try {
      // TODO: should this use getUserFromAuthenticator instead of SessionState.get().getUserName()?
      Function function = new Function(functionName, dbName, desc.getClassName(), SessionState.get().getUserName(),
          PrincipalType.USER, (int) (System.currentTimeMillis() / 1000), FunctionType.JAVA, desc.getResources());
      context.getDb().createFunction(function);
      return true;
    } catch (Exception e) {
      // Addition to metastore failed, remove the function from the registry except if already exists.
      if (!(e.getCause() instanceof AlreadyExistsException)) {
        FunctionRegistry.unregisterPermanentFunction(registeredName);
      }
      context.getTask().setException(e);
      LOG.error("Failed to add function " + desc.getName() + " to the metastore.", e);
      return false;
    }
  }

  private int handlePermanentFunctionCreationException(Exception e) {
    // For repl load flow, function may exist for first incremental phase. So, just return success.
    if (desc.getReplicationSpec().isInReplicationScope() && (e.getCause() instanceof AlreadyExistsException)) {
      LOG.info("Create function is idempotent as function: " + desc.getName() + " already exists.");
      return 0;
    }
    context.getTask().setException(e);
    LOG.error("Failed to create function", e);
    return 1;
  }
}
