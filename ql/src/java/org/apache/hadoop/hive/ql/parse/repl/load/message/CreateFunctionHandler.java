
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
package org.apache.hadoop.hive.ql.parse.repl.load.message;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.ReplChangeManager;
import org.apache.hadoop.hive.metastore.api.ResourceUri;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.function.create.CreateFunctionDesc;
import org.apache.hadoop.hive.ql.exec.FunctionUtils;
import org.apache.hadoop.hive.ql.exec.ReplCopyTask;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.parse.EximUtil;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.repl.PathBuilder;
import org.apache.hadoop.hive.ql.parse.repl.load.MetaData;
import org.apache.hadoop.hive.ql.plan.CopyWork;
import org.apache.hadoop.hive.ql.plan.DependencyCollectionWork;
import org.apache.hadoop.util.Time;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer.toReadEntity;

public class CreateFunctionHandler extends AbstractMessageHandler {
  private String functionName;

  public String getFunctionName() {
    return functionName;
  }

  @Override
  public List<Task<?>> handle(Context context)
      throws SemanticException {
    try {
      FunctionDescBuilder builder = new FunctionDescBuilder(context);
      CreateFunctionDesc descToLoad = builder.build();
      this.functionName = builder.metadata.function.getFunctionName();
      context.log.debug("Loading function desc : {}", descToLoad.toString());
      Task<DDLWork> createTask = TaskFactory.get(
          new DDLWork(readEntitySet, writeEntitySet, descToLoad,
                      true, context.getDumpDirectory(), context.getMetricCollector()), context.hiveConf);
      context.log.debug("Added create function task : {}:{},{}", createTask.getId(),
          descToLoad.getName(), descToLoad.getClassName());
      // This null check is specifically done as the same class is used to handle both incremental and
      // bootstrap replication scenarios for create function. When doing bootstrap we do not have
      // event id for this event but rather when bootstrap started and hence we pass in null dmd for
      // bootstrap.There should be a better way to do this but might required a lot of changes across
      // different handlers, unless this is a common pattern that is seen, leaving this here.
      if (context.dmd != null) {
        updatedMetadata.set(context.dmd.getEventTo().toString(), builder.destinationDbName,
                            null, null);
      }
      readEntitySet.add(toReadEntity(new Path(context.location), context.hiveConf));
      if (builder.replCopyTasks.isEmpty()) {
        // reply copy only happens for jars on hdfs not otherwise.
        return Collections.singletonList(createTask);
      } else {
        /**
         *  This is to understand how task dependencies work.
         *  All root tasks are executed in parallel. For bootstrap replication there should be only one root task of creating db. Incremental can be multiple ( have to verify ).
         *  Task has children, which are put in queue for execution after the parent has finished execution.
         *  One -to- One dependency can be satisfied by adding children to a given task, do this recursively where the relation holds.
         *  for many to one , create a barrier task that is the child of every item in 'many' dependencies, make the 'one' dependency as child of barrier task.
         *  add the 'many' to parent/root tasks. The execution environment will make sure that the child barrier task will not get executed unless all parents of the barrier task are complete,
         *  which should only happen when the last task is finished, at which point the child of the barrier task is picked up.
         */
        Task<?> barrierTask =
            TaskFactory.get(new DependencyCollectionWork(), context.hiveConf);
        builder.replCopyTasks.forEach(t -> t.addDependentTask(barrierTask));
        barrierTask.addDependentTask(createTask);
        return builder.replCopyTasks;
      }
    } catch (Exception e) {
      throw (e instanceof SemanticException)
          ? (SemanticException) e
          : new SemanticException("Error reading message members", e);
    }
  }

  private static class FunctionDescBuilder {
    private final Context context;
    private final MetaData metadata;
    private final String destinationDbName;
    private final List<Task<?>> replCopyTasks = new ArrayList<>();

    private FunctionDescBuilder(Context context) throws SemanticException {
      this.context = context;
      try {
        FileSystem fs = FileSystem.get(new Path(context.location).toUri(), context.hiveConf);
        metadata = EximUtil.readMetaData(fs, new Path(context.location, EximUtil.METADATA_NAME));
      } catch (IOException e) {
        throw new SemanticException(ErrorMsg.INVALID_PATH.getMsg(), e);
      }
      destinationDbName = context.isDbNameEmpty() ? metadata.function.getDbName() : context.dbName;
    }

    private CreateFunctionDesc build() throws SemanticException {
      replCopyTasks.clear();
      PrimaryToReplicaResourceFunction conversionFunction =
          new PrimaryToReplicaResourceFunction(context, metadata, destinationDbName);
      // We explicitly create immutable lists here as it forces the guava lib to run the transformations
      // and not do them lazily. The reason being the function class used for transformations additionally
      // also creates the corresponding replCopyTasks, which cannot be evaluated lazily. since the query
      // plan needs to be complete before we execute and not modify it while execution in the driver.
      List<ResourceUri> transformedUris = (metadata.function.getResourceUris() == null)
              ? null
              : ImmutableList.copyOf(Lists.transform(metadata.function.getResourceUris(), conversionFunction));
      replCopyTasks.addAll(conversionFunction.replCopyTasks);
      String fullQualifiedFunctionName = FunctionUtils.qualifyFunctionName(
          metadata.function.getFunctionName(), destinationDbName
      );
      // For bootstrap load, the create function should be always performed.
      // Only for incremental load, need to validate if event is newer than the database.
      ReplicationSpec replSpec = (context.dmd == null) ? null : context.eventOnlyReplicationSpec();
      return new CreateFunctionDesc(
              fullQualifiedFunctionName, metadata.function.getClassName(), false,
              transformedUris, replSpec
      );
    }
  }

  static class PrimaryToReplicaResourceFunction
      implements Function<ResourceUri, ResourceUri> {
    private final Context context;
    private final MetaData metadata;
    private final List<Task<?>> replCopyTasks = new ArrayList<>();
    private final String functionsRootDir;
    private String destinationDbName;

    PrimaryToReplicaResourceFunction(Context context, MetaData metadata,
        String destinationDbName) {
      this.context = context;
      this.metadata = metadata;
      this.destinationDbName = destinationDbName;
      this.functionsRootDir = context.hiveConf.getVar(HiveConf.ConfVars.REPL_FUNCTIONS_ROOT_DIR);
    }

    @Override
    public ResourceUri apply(ResourceUri resourceUri) {
      try {
        return resourceUri.getUri().toLowerCase().startsWith("hdfs:")
            ? destinationResourceUri(resourceUri)
            : resourceUri;
      } catch (IOException | SemanticException e) {
        throw new RuntimeException(e);
      }
    }

    /**
     * the destination also includes the current timestamp to randomise the placement of the jar at a given location for a function .
     * this is done to allow the  CREATE / DROP / CREATE of the same function with same name and jar's but updated
     * binaries across the two creates.
     */
    ResourceUri destinationResourceUri(ResourceUri resourceUri)
        throws IOException, SemanticException {
      String sourceUri = resourceUri.getUri();
      String[] split = ReplChangeManager.decodeFileUri(sourceUri)[0].split(Path.SEPARATOR);
      PathBuilder pathBuilder = new PathBuilder(functionsRootDir);
      Path qualifiedDestinationPath = PathBuilder.fullyQualifiedHDFSUri(
          pathBuilder
              .addDescendant(destinationDbName.toLowerCase())
              .addDescendant(metadata.function.getFunctionName().toLowerCase())
              .addDescendant(String.valueOf(Time.monotonicNowNanos()))
              .addDescendant(split[split.length - 1])
              .build(),
          new Path(functionsRootDir).getFileSystem(context.hiveConf)
      );

      replCopyTasks.add(getCopyTask(sourceUri, qualifiedDestinationPath));
      ResourceUri destinationUri =
          new ResourceUri(resourceUri.getResourceType(), qualifiedDestinationPath.toString());
      context.log.debug("copy source uri : {} to destination uri: {}", sourceUri, destinationUri);
      return destinationUri;
    }

    private Task<?> getCopyTask(String sourceUri, Path dest) {
      boolean copyAtLoad = context.hiveConf.getBoolVar(HiveConf.ConfVars.REPL_RUN_DATA_COPY_TASKS_ON_TARGET);
      if (copyAtLoad ) {
        return ReplCopyTask.getLoadCopyTask(metadata.getReplicationSpec(), new Path(sourceUri), dest, context.hiveConf,
                context.getDumpDirectory(), context.getMetricCollector());
      } else {
        //CopyTask expects the destination directory, hence we pass the parent of the actual destination path
        return TaskFactory.get(new CopyWork(new Path(sourceUri), dest.getParent(), true, false,
                context.getDumpDirectory(), context.getMetricCollector(), true), context.hiveConf);
      }
    }
  }
}
