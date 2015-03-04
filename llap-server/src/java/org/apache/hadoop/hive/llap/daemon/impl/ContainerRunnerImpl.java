/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.llap.daemon.impl;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.util.AuxiliaryServiceHelper;
import org.apache.log4j.Logger;
import org.apache.log4j.NDC;
import org.apache.tez.common.security.JobTokenIdentifier;
import org.apache.tez.common.security.TokenCache;
import org.apache.hadoop.hive.common.CallableWithNdc;
import org.apache.hadoop.hive.llap.daemon.ContainerRunner;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.RunContainerRequestProto;
import org.apache.tez.dag.api.TezConstants;
import org.apache.tez.runtime.api.ExecutionContext;
import org.apache.tez.runtime.api.impl.ExecutionContextImpl;
import org.apache.tez.runtime.common.objectregistry.ObjectRegistryImpl;
import org.apache.tez.runtime.task.TezChild;
import org.apache.tez.runtime.task.TezChild.ContainerExecutionResult;
import org.apache.hadoop.hive.llap.shufflehandler.ShuffleHandler;

public class ContainerRunnerImpl extends AbstractService implements ContainerRunner {

  private static final Logger LOG = Logger.getLogger(ContainerRunnerImpl.class);

  private final int numExecutors;
  private final ListeningExecutorService executorService;
  private final AtomicReference<InetSocketAddress> localAddress;
  private final String[] localDirsBase;
  private final int localShufflePort;
  private final Map<String, String> localEnv = new HashMap<String, String>();
  private volatile FileSystem localFs;
  private final long memoryPerExecutor;
  // TODO Support for removing queued containers, interrupting / killing specific containers

  public ContainerRunnerImpl(int numExecutors, String[] localDirsBase, int localShufflePort,
                             AtomicReference<InetSocketAddress> localAddress,
                             long totalMemoryAvailableBytes) {
    super("ContainerRunnerImpl");
    Preconditions.checkState(numExecutors > 0,
        "Invalid number of executors: " + numExecutors + ". Must be > 0");
    this.numExecutors = numExecutors;
    this.localDirsBase = localDirsBase;
    this.localShufflePort = localShufflePort;
    this.localAddress = localAddress;

    ExecutorService raw = Executors.newFixedThreadPool(numExecutors,
        new ThreadFactoryBuilder().setNameFormat("ContainerExecutor %d").build());
    this.executorService = MoreExecutors.listeningDecorator(raw);
    AuxiliaryServiceHelper.setServiceDataIntoEnv(
        TezConstants.TEZ_SHUFFLE_HANDLER_SERVICE_ID,
        ByteBuffer.allocate(4).putInt(localShufflePort), localEnv);

    // 80% of memory considered for accounted buffers. Rest for objects.
    // TODO Tune this based on the available size.
    this.memoryPerExecutor = (long)(totalMemoryAvailableBytes * 0.8 / (float) numExecutors);

    LOG.info("ContainerRunnerImpl config: " +
        "memoryPerExecutorDerviced=" + memoryPerExecutor
    );
  }

  @Override
  public void serviceInit(Configuration conf) {
    try {
      localFs = FileSystem.getLocal(conf);
    } catch (IOException e) {
      throw new RuntimeException("Failed to setup local filesystem instance", e);
    }
  }

  @Override
  public void serviceStart() {
  }

  @Override
  protected void serviceStop() throws Exception {
    super.serviceStop();
  }

  // TODO Move this into a utilities class
  private static String createAppSpecificLocalDir(String baseDir, String applicationIdString,
                                                  String user) {
    // TODO This is broken for secure clusters. The app will not have permission to create these directories.
    // May work via Slider - since the directory would already exist. Otherwise may need a custom shuffle handler.
    // TODO This should be the process user - and not the user on behalf of whom the query is being submitted.
    return baseDir + File.separator + "usercache" + File.separator + user + File.separator +
        "appcache" + File.separator + applicationIdString;
  }

  @Override
  public void queueContainer(RunContainerRequestProto request) throws IOException {
    LOG.info("Queing container for execution: " + request);
    // This is the start of container-annotated logging.
    NDC.push(request.getContainerIdString());
    try {
      Map<String, String> env = new HashMap<String, String>();
      // TODO What else is required in this environment map.
      env.putAll(localEnv);
      env.put(ApplicationConstants.Environment.USER.name(), request.getUser());

      String[] localDirs = new String[localDirsBase.length];

      // Setup up local dirs to be application specific, and create them.
      for (int i = 0; i < localDirsBase.length; i++) {
        localDirs[i] = createAppSpecificLocalDir(localDirsBase[i], request.getApplicationIdString(),
            request.getUser());
        localFs.mkdirs(new Path(localDirs[i]));
      }
      LOG.info("DEBUG: Dirs are: " + Arrays.toString(localDirs));


      // Setup workingDir. This is otherwise setup as Environment.PWD
      // Used for re-localization, to add the user specified configuration (conf_pb_binary_stream)
      // TODO Set this up to read user configuration if required. Ideally, Inputs / Outputs should be self configured.
      // Setting this up correctly is more from framework components to setup security, ping intervals, etc.
      String workingDir = localDirs[0];

      Credentials credentials = new Credentials();
      DataInputBuffer dib = new DataInputBuffer();
      byte[] tokenBytes = request.getCredentialsBinary().toByteArray();
      dib.reset(tokenBytes, tokenBytes.length);
      credentials.readTokenStorageStream(dib);

      Token<JobTokenIdentifier> jobToken = TokenCache.getSessionToken(credentials);

      // TODO Unregistering does not happen at the moment, since there's no signals on when an app completes.
      LOG.info("DEBUG: Registering request with the ShuffleHandler");
      ShuffleHandler.get().registerApplication(request.getApplicationIdString(), jobToken, request.getUser());


      ContainerRunnerCallable callable = new ContainerRunnerCallable(request, new Configuration(getConfig()),
          new ExecutionContextImpl(localAddress.get().getHostName()), env, localDirs,
          workingDir, credentials, memoryPerExecutor);
      ListenableFuture<ContainerExecutionResult> future = executorService
          .submit(callable);
      Futures.addCallback(future, new ContainerRunnerCallback(request, callable));
    } finally {
      NDC.pop();
    }
  }

  static class ContainerRunnerCallable extends CallableWithNdc<ContainerExecutionResult> {

    private final RunContainerRequestProto request;
    private final Configuration conf;
    private final String workingDir;
    private final String[] localDirs;
    private final Map<String, String> envMap;
    // TODO Is a null pid valid - will this work with multiple different ResourceMonitors ?
    private final String pid = null;
    private final ObjectRegistryImpl objectRegistry;
    private final ExecutionContext executionContext;
    private final Credentials credentials;
    private final long memoryAvailable;
    private volatile TezChild tezChild;


    ContainerRunnerCallable(RunContainerRequestProto request, Configuration conf,
                            ExecutionContext executionContext, Map<String, String> envMap,
                            String[] localDirs, String workingDir, Credentials credentials,
                            long memoryAvailable) {
      this.request = request;
      this.conf = conf;
      this.executionContext = executionContext;
      this.envMap = envMap;
      this.workingDir = workingDir;
      this.localDirs = localDirs;
      this.objectRegistry = new ObjectRegistryImpl();
      this.credentials = credentials;
      this.memoryAvailable = memoryAvailable;

    }

    @Override
    protected ContainerExecutionResult callInternal() throws Exception {
      Stopwatch sw = new Stopwatch().start();
      tezChild =
          new TezChild(conf, request.getAmHost(), request.getAmPort(),
              request.getContainerIdString(),
              request.getTokenIdentifier(), request.getAppAttemptNumber(), workingDir, localDirs,
              envMap, objectRegistry, pid,
              executionContext, credentials, memoryAvailable, request.getUser(), null);
      ContainerExecutionResult result = tezChild.run();
      LOG.info("ExecutionTime for Container: " + request.getContainerIdString() + "=" +
          sw.stop().elapsedMillis());
      return result;
    }

    public TezChild getTezChild() {
      return this.tezChild;
    }
  }

  final class ContainerRunnerCallback implements FutureCallback<ContainerExecutionResult> {

    private final RunContainerRequestProto request;
    private final ContainerRunnerCallable containerRunnerCallable;

    ContainerRunnerCallback(RunContainerRequestProto request,
                            ContainerRunnerCallable containerRunnerCallable) {
      this.request = request;
      this.containerRunnerCallable = containerRunnerCallable;
    }

    // TODO Slightly more useful error handling
    @Override
    public void onSuccess(ContainerExecutionResult result) {
      switch (result.getExitStatus()) {
        case SUCCESS:
          LOG.info("Successfully finished: " + request.getApplicationIdString() + ", containerId=" +
              request.getContainerIdString());
          break;
        case EXECUTION_FAILURE:
          LOG.info("Failed to run: " + request.getApplicationIdString() + ", containerId=" +
              request.getContainerIdString(), result.getThrowable());
          break;
        case INTERRUPTED:
          LOG.info(
              "Interrupted while running: " + request.getApplicationIdString() + ", containerId=" +
                  request.getContainerIdString(), result.getThrowable());
          break;
        case ASKED_TO_DIE:
          LOG.info(
              "Asked to die while running: " + request.getApplicationIdString() + ", containerId=" +
                  request.getContainerIdString());
          break;
      }
    }

    @Override
    public void onFailure(Throwable t) {
      LOG.error(
          "TezChild execution failed for : " + request.getApplicationIdString() + ", containerId=" +
              request.getContainerIdString());
      TezChild tezChild = containerRunnerCallable.getTezChild();
      if (tezChild != null) {
        tezChild.shutdown();
      }
    }
  }
}
