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

package org.apache.hadoop.hive.ql.exec;

import static org.apache.hadoop.util.StringUtils.stringifyException;

import java.io.IOException;
import java.util.List;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.ResourceType;
import org.apache.hadoop.hive.metastore.api.ResourceUri;
import org.apache.hadoop.hive.ql.exec.FunctionInfo.FunctionResource;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.CreateFunctionDesc;
import org.apache.hadoop.hive.ql.plan.DropFunctionDesc;
import org.apache.hadoop.hive.ql.plan.CreateMacroDesc;
import org.apache.hadoop.hive.ql.plan.DropMacroDesc;
import org.apache.hadoop.hive.ql.plan.FunctionWork;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.util.ResourceDownloader;
import org.apache.hadoop.util.StringUtils;

/**
 * FunctionTask.
 *
 */
public class FunctionTask extends Task<FunctionWork> {
  private static final long serialVersionUID = 1L;
  private static transient final Logger LOG = LoggerFactory.getLogger(FunctionTask.class);

  public FunctionTask() {
    super();
  }

  @Override
  public void initialize(QueryState queryState, QueryPlan queryPlan, DriverContext ctx,
      CompilationOpContext opContext) {
    super.initialize(queryState, queryPlan, ctx, opContext);
  }

  @Override
  public int execute(DriverContext driverContext) {
    CreateFunctionDesc createFunctionDesc = work.getCreateFunctionDesc();
    if (createFunctionDesc != null) {
      if (createFunctionDesc.isTemp()) {
        return createTemporaryFunction(createFunctionDesc);
      } else {
        try {
          return createPermanentFunction(Hive.get(conf), createFunctionDesc);
        } catch (Exception e) {
          setException(e);
          LOG.error(stringifyException(e));
          return 1;
        }
      }
    }

    DropFunctionDesc dropFunctionDesc = work.getDropFunctionDesc();
    if (dropFunctionDesc != null) {
      if (dropFunctionDesc.isTemp()) {
        return dropTemporaryFunction(dropFunctionDesc);
      } else {
        try {
          return dropPermanentFunction(Hive.get(conf), dropFunctionDesc);
        } catch (Exception e) {
          setException(e);
          LOG.error(stringifyException(e));
          return 1;
        }
      }
    }

    if (work.getReloadFunctionDesc() != null) {
      try {
        Hive.get().reloadFunctions();
      } catch (Exception e) {
        setException(e);
        LOG.error(stringifyException(e));
        return 1;
      }
    }

    CreateMacroDesc createMacroDesc = work.getCreateMacroDesc();
    if (createMacroDesc != null) {
      return createMacro(createMacroDesc);
    }

    DropMacroDesc dropMacroDesc = work.getDropMacroDesc();
    if (dropMacroDesc != null) {
      return dropMacro(dropMacroDesc);
    }
    return 0;
  }

  // todo authorization
  private int createPermanentFunction(Hive db, CreateFunctionDesc createFunctionDesc)
      throws HiveException, IOException {
    String[] qualifiedNameParts = FunctionUtils.getQualifiedFunctionNameParts(
        createFunctionDesc.getFunctionName());
    String dbName = qualifiedNameParts[0];
    String funcName = qualifiedNameParts[1];
    String registeredName = FunctionUtils.qualifyFunctionName(funcName, dbName);
    String className = createFunctionDesc.getClassName();

    List<ResourceUri> resources = createFunctionDesc.getResources();

    // For permanent functions, check for any resources from local filesystem.
    checkLocalFunctionResources(db, createFunctionDesc.getResources());

    FunctionInfo registered = null;
    try {
      registered = FunctionRegistry.registerPermanentFunction(
        registeredName, className, true, toFunctionResource(resources));
    } catch (RuntimeException ex) {
      Throwable t = ex;
      while (t.getCause() != null) {
        t = t.getCause();
      }
    }
    if (registered == null) {
      console.printError("Failed to register " + registeredName
          + " using class " + createFunctionDesc.getClassName());
      return 1;
    }

    // Add to metastore
    Function func = new Function(
        funcName,
        dbName,
        className,
        SessionState.get().getUserName(),
        PrincipalType.USER,
        (int) (System.currentTimeMillis() / 1000),
        org.apache.hadoop.hive.metastore.api.FunctionType.JAVA,
        resources
    );
    db.createFunction(func);
    return 0;
  }

  private int createTemporaryFunction(CreateFunctionDesc createFunctionDesc) {
    try {
      // Add any required resources
      FunctionResource[] resources = toFunctionResource(createFunctionDesc.getResources());
      addFunctionResources(resources);

      Class<?> udfClass = getUdfClass(createFunctionDesc);
      FunctionInfo registered = FunctionRegistry.registerTemporaryUDF(
          createFunctionDesc.getFunctionName(), udfClass, resources);
      if (registered != null) {
        return 0;
      }
      console.printError("FAILED: Class " + createFunctionDesc.getClassName()
          + " does not implement UDF, GenericUDF, or UDAF");
      return 1;
    } catch (HiveException e) {
      console.printError("FAILED: " + e.toString());
      LOG.info("create function: " + StringUtils.stringifyException(e));
      return 1;
    } catch (ClassNotFoundException e) {

      console.printError("FAILED: Class " + createFunctionDesc.getClassName() + " not found");
      LOG.info("create function: " + StringUtils.stringifyException(e));
      return 1;
    }
  }

  private int createMacro(CreateMacroDesc createMacroDesc) {
    FunctionRegistry.registerTemporaryMacro(
      createMacroDesc.getMacroName(),
      createMacroDesc.getBody(),
      createMacroDesc.getColNames(),
      createMacroDesc.getColTypes()
    );
    return 0;
  }

  private int dropMacro(DropMacroDesc dropMacroDesc) {
    try {
      FunctionRegistry.unregisterTemporaryUDF(dropMacroDesc.getMacroName());
      return 0;
    } catch (HiveException e) {
      LOG.info("drop macro: " + StringUtils.stringifyException(e));
      return 1;
    }
  }

  // todo authorization
  private int dropPermanentFunction(Hive db, DropFunctionDesc dropFunctionDesc) {
    try {
      String[] qualifiedNameParts = FunctionUtils.getQualifiedFunctionNameParts(
          dropFunctionDesc.getFunctionName());
      String dbName = qualifiedNameParts[0];
      String funcName = qualifiedNameParts[1];

      String registeredName = FunctionUtils.qualifyFunctionName(funcName, dbName);
      FunctionRegistry.unregisterPermanentFunction(registeredName);
      db.dropFunction(dbName, funcName);

      return 0;
    } catch (Exception e) {
      LOG.info("drop function: " + StringUtils.stringifyException(e));
      console.printError("FAILED: error during drop function: " + StringUtils.stringifyException(e));
      return 1;
    }
  }

  private int dropTemporaryFunction(DropFunctionDesc dropFunctionDesc) {
    try {
      FunctionRegistry.unregisterTemporaryUDF(dropFunctionDesc.getFunctionName());
      return 0;
    } catch (HiveException e) {
      LOG.info("drop function: " + StringUtils.stringifyException(e));
      return 1;
    }
  }

  private void checkLocalFunctionResources(Hive db, List<ResourceUri> resources)
      throws HiveException {
    // If this is a non-local warehouse, then adding resources from the local filesystem
    // may mean that other clients will not be able to access the resources.
    // So disallow resources from local filesystem in this case.
    if (resources != null && resources.size() > 0) {
      try {
        String localFsScheme = FileSystem.getLocal(db.getConf()).getUri().getScheme();
        String configuredFsScheme = FileSystem.get(db.getConf()).getUri().getScheme();
        if (configuredFsScheme.equals(localFsScheme)) {
          // Configured warehouse FS is local, don't need to bother checking.
          return;
        }

        for (ResourceUri res : resources) {
          String resUri = res.getUri();
          if (ResourceDownloader.isFileUri(resUri)) {
            throw new HiveException("Hive warehouse is non-local, but "
                + res.getUri() + " specifies file on local filesystem. "
                + "Resources on non-local warehouse should specify a non-local scheme/path");
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

  public static FunctionResource[] toFunctionResource(List<ResourceUri> resources)
      throws HiveException {
    if (resources == null) {
      return null;
    }
    FunctionResource[] converted = new FunctionResource[resources.size()];
    for (int i = 0; i < converted.length; i++) {
      ResourceUri resource = resources.get(i);
      SessionState.ResourceType type = getResourceType(resource.getResourceType());
      converted[i] = new FunctionResource(type, resource.getUri());
    }
    return converted;
  }

  public static SessionState.ResourceType getResourceType(ResourceType rt) {
    switch (rt) {
      case JAR:
        return SessionState.ResourceType.JAR;
      case FILE:
        return SessionState.ResourceType.FILE;
      case ARCHIVE:
        return SessionState.ResourceType.ARCHIVE;
      default:
        throw new AssertionError("Unexpected resource type " + rt);
    }
  }

  public static void addFunctionResources(FunctionResource[] resources) throws HiveException {
    if (resources != null) {
      Multimap<SessionState.ResourceType, String> mappings = HashMultimap.create();
      for (FunctionResource res : resources) {
        mappings.put(res.getResourceType(), res.getResourceURI());
      }
      for (SessionState.ResourceType type : mappings.keys()) {
        SessionState.get().add_resources(type, mappings.get(type));
      }
    }
  }

  private Class<?> getUdfClass(CreateFunctionDesc desc) throws ClassNotFoundException {
    // get the session specified class loader from SessionState
    ClassLoader classLoader = Utilities.getSessionSpecifiedClassLoader();
    return Class.forName(desc.getClassName(), true, classLoader);
  }

  @Override
  public StageType getType() {
    return StageType.FUNC;
  }

  @Override
  public String getName() {
    return "FUNCTION";
  }
}
