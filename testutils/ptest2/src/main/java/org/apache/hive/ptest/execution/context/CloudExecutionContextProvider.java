/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.ptest.execution.context;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.hive.ptest.execution.Constants;
import org.apache.hive.ptest.execution.Dirs;
import org.apache.hive.ptest.execution.LocalCommandFactory;
import org.apache.hive.ptest.execution.conf.Context;
import org.apache.hive.ptest.execution.conf.Host;
import org.apache.hive.ptest.execution.ssh.SSHCommand;
import org.apache.hive.ptest.execution.ssh.SSHCommandExecutor;
import org.jclouds.compute.RunNodesException;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.domain.Credentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class CloudExecutionContextProvider implements ExecutionContextProvider {
  private static final Logger LOG = LoggerFactory
      .getLogger(CloudExecutionContextProvider.class);
  public static final String CLOUD_PROVIDER = "cloudProvider";

  // GCE settings
  public static final String GCE_JSON_CREDS_FILE = "gceJsonFile";
  
  // AWS settings
  public static final String API_KEY = "apiKey";
  public static final String ACCESS_KEY = "accessKey";
  public static final String KEY_PAIR = "keyPair";
  public static final String MAX_BID = "maxBid";
  
  // Generic settings
  public static final String DATA_DIR = "dataDir";
  public static final String NUM_HOSTS = "numHosts";
  public static final String MAX_HOSTS_PER_CREATE_REQUEST = "maxHostsPerCreateRequest";
  public static final String GROUP_NAME = "groupName";
  public static final String IMAGE_ID = "imageId";
  public static final String SECURITY_GROUP = "securityGroup";
  public static final String SLAVE_LOCAL_DIRECTORIES = "localDirs";
  public static final String USERNAME = "user";
  public static final String INSTANCE_TYPE = "instanceType";
  public static final String NUM_THREADS = "numThreads";
  public static final String USER_METADATA = "userMetadata";

  private final RandomAccessFile mHostLog;
  private final String mPrivateKey;
  private final String mUser;
  private final String[] mSlaveLocalDirs;
  private final int mNumThreads;
  private final int mNumHosts;
  private final int mMaxHostsPerCreateRequest;
  private final long mRetrySleepInterval;
  private final CloudComputeService mCloudComputeService;
  private final Map<String, Long> mTerminatedHosts;
  private final Map<String, Long> mLiveHosts;
  private final ExecutorService mTerminationExecutor;
  private final File mWorkingDir;
  private final SSHCommandExecutor mSSHCommandExecutor;

  @VisibleForTesting
  CloudExecutionContextProvider(String dataDir,
      int numHosts, CloudComputeService cloudComputeService, SSHCommandExecutor sshCommandExecutor,
      String workingDirectory, String privateKey, String user, String[] slaveLocalDirs, int numThreads,
      long retrySleepInterval, int maxHostsPerCreateRequest) throws IOException {
    mNumHosts = numHosts;
    mMaxHostsPerCreateRequest = maxHostsPerCreateRequest;
    mCloudComputeService = cloudComputeService;
    mPrivateKey = privateKey;
    mUser = user;
    mSlaveLocalDirs = slaveLocalDirs;
    mNumThreads = numThreads;
    mRetrySleepInterval = retrySleepInterval;
    mSSHCommandExecutor = sshCommandExecutor;
    mWorkingDir = Dirs.create(new File(workingDirectory, "working"));
    mLiveHosts = Collections.synchronizedMap(new HashMap<String, Long>());
    mTerminatedHosts = Collections
        .synchronizedMap(new LinkedHashMap<String, Long>() {
          private static final long serialVersionUID = 1L;

          @Override
          public boolean removeEldestEntry(Map.Entry<String, Long> entry) {
            return size() > 100;
          }
        });
    mTerminationExecutor = Executors.newSingleThreadExecutor();
    mHostLog = new RandomAccessFile(new File(dataDir, "hosts"), "rw");
    initialize();
  }

  private void initialize() throws IOException {
    LOG.info("CloudExecutionContextProvider maxHostsPerCreateRequest = " + mMaxHostsPerCreateRequest);
    Set<String> hosts = Sets.newHashSet();
    String host = null;
    mHostLog.seek(0); // should already be true
    while ((host = mHostLog.readLine()) != null) {
      hosts.add(host.trim());
    }
    if (!hosts.isEmpty()) {
      terminate(hosts, true);
    }
    mHostLog.seek(0);
    mHostLog.setLength(0);
    Thread thread = new Thread() {
      @Override
      public void run() {
        while (true) {
          try {
            TimeUnit.MINUTES.sleep(60);
            performBackgroundWork();
          } catch (Exception e) {
            LOG.error("Unexpected error in background worker", e);
          }
        }
      }
    };
    thread.setDaemon(true);
    thread.setName(getClass().getSimpleName() + "-BackgroundWorker");
    thread.start();
  }

  @Override
  public synchronized void terminate(ExecutionContext executionContext) {
    Set<String> hostsToTerminate = Sets.newHashSet();
    for (Host host : executionContext.getHosts()) {
      hostsToTerminate.add(host.getName());
    }
    terminate(hostsToTerminate, true);
  }
  @Override
  public void replaceBadHosts(ExecutionContext executionContext)
      throws CreateHostsFailedException {
    Set<String> hostsToTerminate = Sets.newHashSet();
    Set<Host> hostsNotRemoved = Sets.newHashSet();
    for(Host host : executionContext.getBadHosts()) {
      hostsToTerminate.add(host.getName());
      if(!executionContext.removeHost(host)) {
        hostsNotRemoved.add(host);
      }
    }
    executionContext.clearBadHosts();
    if(!hostsToTerminate.isEmpty()) {
      LOG.info("Replacing " + hostsToTerminate);
      terminate(hostsToTerminate, true);
      Set<NodeMetadata> nodes = createNodes(hostsToTerminate.size());
      for (NodeMetadata node : nodes) {
        executionContext.addHost(new Host(publicIp(node), mUser, mSlaveLocalDirs,
            mNumThreads));
      }
    }
    Preconditions.checkState(hostsNotRemoved.isEmpty(),
        "Host " + hostsNotRemoved + " was in bad hosts but could not be removed");
  }

  @Override
  public synchronized ExecutionContext createExecutionContext()
      throws CreateHostsFailedException, ServiceNotAvailableException {
    try {
      Set<NodeMetadata> nodes = createNodes(mNumHosts);
      Set<Host> hosts = Sets.newHashSet();
      for (NodeMetadata node : nodes) {
        hosts.add(new Host(publicIp(node), mUser, mSlaveLocalDirs,
          mNumThreads));
      }
      return new ExecutionContext(this, hosts, mWorkingDir.getAbsolutePath(),
          mPrivateKey);
    } finally {
      syncLog();
    }
  }

  private Set<NodeMetadata> createNodes(final int numHosts)
      throws CreateHostsFailedException {
    Set<NodeMetadata> result = Sets.newHashSet();
    int attempts = 0;
    int numRequired = numHosts;
    // pause so we don't get banned
    try {
      TimeUnit.SECONDS.sleep(mRetrySleepInterval);
    } catch (InterruptedException e) {
     Thread.currentThread().interrupt();
    }
    do {
      boolean error = false;
      LOG.info("Attempting to create " + numRequired + " nodes");
      try {
        result.addAll(mCloudComputeService.createNodes(Math.min(mMaxHostsPerCreateRequest, numRequired)));

        Set<String> newAddresses = new HashSet<String>();
        for (NodeMetadata node : result) {
          newAddresses.addAll(node.getPublicAddresses());
        }
        synchronized (mTerminatedHosts) {
          for (String newAddress : newAddresses) {
            mTerminatedHosts.remove(newAddress);
          }
        }
      } catch (RunNodesException e) {
        error = true;
        LOG.warn("Error creating nodes", e);
        terminateInternal(e.getNodeErrors().keySet());
        result.addAll(e.getSuccessfulNodes());
      }
      result = verifyHosts(result);
      for (NodeMetadata node : result) {
        mLiveHosts.put(publicIpOrHostname(node), System.currentTimeMillis());
      }
      LOG.info("Successfully created " + result.size() + " nodes");
      numRequired = numHosts - result.size();
      if (numRequired > 0) {
        long sleepTime = mRetrySleepInterval;
        if (error) {
          sleepTime *= ++attempts;
        }
        LOG.info("Pausing creation process for " + sleepTime + " seconds");
        try {
          TimeUnit.SECONDS.sleep(sleepTime);
        } catch(InterruptedException e) {
          throw new CreateHostsFailedException("Interrupted while trying to create hosts", e);
        }
      }
    } while(numRequired > 0);
    Preconditions.checkState(result.size() >= numHosts,
        "Results should always be >= numHosts " + numHosts + " => " + result.size());
    return result;
  }

  @Override
  public void close() {
    LOG.info("Shutting down TerminationExecutor");
    mTerminationExecutor.shutdown();
    LOG.info("Closing CloudComputeService");
    mCloudComputeService.close();
    // I don't entirely believe that everything is cleaned up
    // when close is called based on watching the TRACE logs
    try {
      TimeUnit.SECONDS.sleep(10);
    } catch (InterruptedException e) {
      // ignore, shutting down anyway
    }
  }


  private static String publicIpOrHostname(NodeMetadata node) {
    Set<String> publicIps = node.getPublicAddresses();
    if (publicIps.size() == 1) {
      return Iterables.getOnlyElement(publicIps);
    }
    return node.getHostname();
  }

  private static String publicIp(NodeMetadata node) {
    Set<String> publicIps = node.getPublicAddresses();
    if (publicIps.size() == 1) {
      return Iterables.getOnlyElement(publicIps);
    }
    throw new IllegalStateException("Node does not have exactly one public ip: " + node);
  }

  private Set<NodeMetadata> verifyHosts(Set<? extends NodeMetadata> hosts)
      throws CreateHostsFailedException {
    final Set<NodeMetadata> result = Collections.synchronizedSet(new HashSet<NodeMetadata>());
    if(!hosts.isEmpty()) {
      persistHostnamesToLog(hosts);
      ExecutorService executorService = Executors.newFixedThreadPool(Math.min(hosts.size(), 25));
      try {
        for(final NodeMetadata node : hosts) {
          executorService.submit(new Runnable() {
            @Override
            public void run() {
              String ip = publicIpOrHostname(node);
              SSHCommand command = new SSHCommand(mSSHCommandExecutor, mPrivateKey, mUser, ip, 0, "pkill -f java", true);
              mSSHCommandExecutor.execute(command);
              if(command.getExitCode() == Constants.EXIT_CODE_UNKNOWN ||
                  command.getException() != null) {
                LOG.error("Node " + node + " is bad on startup", command.getException());
                terminateInternal(node);
              } else {
                result.add(node);
              }
            }
          });
        }
        executorService.shutdown();
        if(!executorService.awaitTermination(10, TimeUnit.MINUTES)) {
          LOG.error("Verify command still executing on a host after 10 minutes");
        }
      } catch (InterruptedException e) {
        terminateInternal(result);
        throw new CreateHostsFailedException("Interrupted while trying to create hosts", e);
      } finally {
        if(!executorService.isShutdown()) {
          executorService.shutdownNow();
        }
      }
    }
    return result;
  }


  private synchronized void performBackgroundWork() {
    LOG.info("Performing background work");
    Map<String, Long> terminatedHosts = Maps.newHashMap();
    synchronized (mTerminatedHosts) {
      terminatedHosts.putAll(mTerminatedHosts);
    }
    LOG.info("Currently tracked terminated hosts: {}", terminatedHosts.keySet().toString());
    for (NodeMetadata node : getRunningNodes()) {
      String ip = publicIpOrHostname(node);
      if (terminatedHosts.containsKey(ip)) {
        terminateInternal(node);
        LOG.warn("Found zombie node: " + node + " previously terminated at "
            + new Date(terminatedHosts.get(ip)));
      } else if(!mLiveHosts.containsKey(ip)) {
        LOG.warn("Found zombie node: " + node + " previously unknown to ptest");
      }
    }
  }

  private Set<NodeMetadata> getRunningNodes() {
    Set<NodeMetadata> result = Sets.newHashSet();
    Set<NodeMetadata> computes = mCloudComputeService.listRunningNodes();
    for (NodeMetadata node : computes) {
      result.add(node);
    }
    return result;
  }

  private void terminateInternal(Set<? extends NodeMetadata> nodes) {
    for (NodeMetadata node : nodes) {
      terminateInternal(node);
    }
  }

  private void terminateInternal(final NodeMetadata node) {
    LOG.info("Submitting termination for " + node);
    mLiveHosts.remove(publicIpOrHostname(node));
    mTerminationExecutor.submit(new Runnable() {
      @Override
      public void run() {
        // pause so we don't get banned
        try {
          TimeUnit.SECONDS.sleep(mRetrySleepInterval);
        } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
        }
        try {
          String ip = publicIpOrHostname(node);
          LOG.info("Terminating " + ip);
          if (!mTerminatedHosts.containsKey(ip)) {
            mTerminatedHosts.put(ip, System.currentTimeMillis());
          }
          mCloudComputeService.destroyNode(node.getId());
        } catch (Exception e) {
          LOG.error("Error attempting to terminate host " + node, e);
        }
      }
    });
  }

  private void persistHostnamesToLog(Set<? extends NodeMetadata> nodes) {
    for (NodeMetadata node : nodes) {
      try {
        String ip = publicIpOrHostname(node);
        if(!Strings.nullToEmpty(ip).trim().isEmpty()) {
          mHostLog.writeBytes(ip + "\n");
        }
      } catch (IOException e) {
        Throwables.propagate(e);
      }
    }
  }

  private void syncLog() {
    try {
      mHostLog.getFD().sync();
    } catch (IOException e) {
      Throwables.propagate(e);
    }
  }

  private void terminate(Set<String> hosts, boolean warnIfHostsNotFound) {
    LOG.info("Requesting termination of " + hosts);
    Set<NodeMetadata> nodesToTerminate = Sets.newHashSet();
    for (NodeMetadata node : getRunningNodes()) {
      String ip = publicIpOrHostname(node);
      if (hosts.contains(ip)) {
        nodesToTerminate.add(node);
      }
    }
    terminateInternal(nodesToTerminate);
    if (warnIfHostsNotFound && nodesToTerminate.size() != hosts.size()) {
      LOG.error("Requested termination of " + hosts.size() + " but found only "
          + nodesToTerminate.size());
    }
  }

  public static class Builder implements ExecutionContextProvider.Builder {
    @Override
    public ExecutionContextProvider build(Context context,
        String workingDirectory) throws Exception {
      return create(context, workingDirectory);
    }
  }
  
  private static CloudComputeService createAwsService(final Context context) {
    String apiKey = Preconditions.checkNotNull(context.getString(API_KEY), API_KEY + " is required");
    String accessKey = Preconditions.checkNotNull(context.getString(ACCESS_KEY), ACCESS_KEY + " is required");
    String imageId = Preconditions.checkNotNull(context.getString(IMAGE_ID), IMAGE_ID + " is required");
    String keyPair = Preconditions.checkNotNull(context.getString(KEY_PAIR), KEY_PAIR + " is required");
    String securityGroup = Preconditions.checkNotNull(context.getString(SECURITY_GROUP), SECURITY_GROUP + " is required");

    Float maxBid = context.getFloat(MAX_BID);
    Preconditions.checkArgument(maxBid == null || maxBid > 0, MAX_BID + " must be null or greater than zero");

    String instanceType = context.getString(INSTANCE_TYPE, "c1.xlarge");
    String groupName = context.getString(GROUP_NAME, "hive-ptest-slaves");

    CloudComputeService.CloudComputeConfig config =
            new CloudComputeService.CloudComputeConfig(CloudComputeService.CloudComputeConfig.CloudComputeProvider.AWS);

    config.setCredentials(apiKey, accessKey);
    config.setInstanceType(instanceType);
    config.setGroupName(groupName);
    config.setImageId(imageId);
    config.setmKeyPairName(keyPair);
    config.setSecurityGroup(securityGroup);
    config.setMaxBid(maxBid);
    config.setUserMetaData(context.getSubProperties(USER_METADATA + "."));

    return new CloudComputeService(config);
  }
  
  private static CloudComputeService createGceService(final Context context) throws IOException {
    String gceJsonFile = Preconditions.checkNotNull(context.getString(GCE_JSON_CREDS_FILE), GCE_JSON_CREDS_FILE + " is required");
    String imageId = Preconditions.checkNotNull(context.getString(IMAGE_ID), IMAGE_ID + " is required");
    String securityGroup = Preconditions.checkNotNull(context.getString(SECURITY_GROUP), SECURITY_GROUP + " is required");
    String instanceType = Preconditions.checkNotNull(context.getString(INSTANCE_TYPE, ""), INSTANCE_TYPE + " is required");

    String groupName = context.getString(GROUP_NAME, "hive-ptest-slaves");

    CloudComputeService.CloudComputeConfig config =
            new CloudComputeService.CloudComputeConfig(CloudComputeService.CloudComputeConfig.CloudComputeProvider.GCE);

    Credentials creds = 
            CloudComputeService.getCredentialsFromJsonKeyFile(gceJsonFile);

    config.setCredentials(creds.identity, creds.credential);
    config.setInstanceType(instanceType);
    config.setGroupName(groupName);
    config.setImageId(imageId);
    config.setSecurityGroup(securityGroup);
    config.setUserMetaData(context.getSubProperties(USER_METADATA + "."));

    return new CloudComputeService(config);
  }

  private static CloudComputeService createService(final Context context) throws IOException {
    String cloudProvider = context.getString(CLOUD_PROVIDER, "aws-ec2");
    
    if (cloudProvider.equalsIgnoreCase("aws-ec2")) {
      return createAwsService(context);
    } else if (cloudProvider.equalsIgnoreCase("google-compute-engine")) {
      return createGceService(context);
    } else {
      throw new IllegalArgumentException("Unknown cloud provider name: " + cloudProvider);
    }
  }

  private static CloudExecutionContextProvider create(Context context,
      String workingDirectory) throws IOException {
    String dataDir = Preconditions.checkNotNull(context.getString(DATA_DIR),
        DATA_DIR + " is required");
    int maxHostsPerCreateRequest = context.getInteger(MAX_HOSTS_PER_CREATE_REQUEST, 2);
    Integer numHosts = context.getInteger(NUM_HOSTS, 8);
    Preconditions.checkArgument(numHosts > 0, NUM_HOSTS
        + " must be greater than zero");
    String privateKey = Preconditions.checkNotNull(
        context.getString(PRIVATE_KEY), PRIVATE_KEY + " is required");
    String user = context.getString(USERNAME, "hiveptest");
    String[] localDirs = Iterables.toArray(Splitter.on(",").trimResults()
        .split(context.getString(SLAVE_LOCAL_DIRECTORIES, "/home/hiveptest/")),
        String.class);
    Integer numThreads = context.getInteger(NUM_THREADS, 3);
    
    CloudComputeService cloudComputeService = createService(context);
    CloudExecutionContextProvider service = new CloudExecutionContextProvider(
        dataDir, numHosts, cloudComputeService,
        new SSHCommandExecutor(LOG, new LocalCommandFactory(LOG), "-o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no"),
        workingDirectory, privateKey, user, localDirs, numThreads, 60, maxHostsPerCreateRequest);
    return service;
  }
}
