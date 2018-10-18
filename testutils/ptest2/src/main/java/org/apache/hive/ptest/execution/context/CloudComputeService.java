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
import java.util.*;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import org.jclouds.Constants;
import org.jclouds.ContextBuilder;
import org.jclouds.aws.ec2.compute.AWSEC2TemplateOptions;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.ComputeServiceContext;
import org.jclouds.compute.RunNodesException;
import org.jclouds.compute.config.ComputeServiceProperties;
import org.jclouds.compute.domain.ComputeMetadata;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.compute.domain.NodeMetadata.Status;
import org.jclouds.compute.domain.Template;
import org.jclouds.compute.options.TemplateOptions;
import org.jclouds.domain.Credentials;
import org.jclouds.googlecloud.GoogleCredentialsFromJson;
import org.jclouds.googlecomputeengine.compute.options.GoogleComputeEngineTemplateOptions;
import org.jclouds.logging.log4j.config.Log4JLoggingModule;
import org.jclouds.sshj.config.SshjSshClientModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import static com.google.common.base.Charsets.UTF_8;

public class CloudComputeService {
  private static final Logger LOG = LoggerFactory
      .getLogger(CloudComputeService.class);
  private final CloudComputeConfig mConfig;
  private final ComputeServiceContext mComputeServiceContext;
  private final ComputeService mComputeService;
  private final Template mTemplate;
  private final String mGroupTag;

  public CloudComputeService(final CloudComputeConfig config) {
    mConfig = config;

    mComputeServiceContext = initComputeServiceContext(config.getmProvider(), config.getIdentity(), config.getCredential());
    mComputeService = mComputeServiceContext.getComputeService();
    mTemplate = mComputeService.templateBuilder().hardwareId(config.getInstanceType()).imageId(config.getImageId()).build();

    TemplateOptions options = mTemplate.getOptions();

    // Set generic options
    options.blockOnPort(22, 60);
    options.userMetadata(config.getUserMetaData());

    // Set provider options
    switch (config.getmProvider()) {
      case AWS:
        mGroupTag = String.format("group=%s", config.getGroupName());

        options.as(AWSEC2TemplateOptions.class)
                .keyPair(config.getKeyPairName())
                .securityGroupIds(config.getSecurityGroup())
                .spotPrice(config.getMaxBid())
                .tags(Collections.singletonList(mGroupTag));
        break;
      case GCE:
        mGroupTag = config.getGroupName();

        options.as(GoogleComputeEngineTemplateOptions.class)
                .tags(Arrays.asList(config.getGroupName(), config.getSecurityGroup())); // GCE firewall is set through instance tags
        break;
      default:
        mGroupTag = "";
    }
  }

  private ComputeServiceContext initComputeServiceContext(CloudComputeConfig.CloudComputeProvider provider, String identity, String credential) {
    Properties overrides = new Properties();

    overrides.put(ComputeServiceProperties.POLL_INITIAL_PERIOD, String.valueOf(60L * 1000L));
    overrides.put(ComputeServiceProperties.POLL_MAX_PERIOD, String.valueOf(600L * 1000L));
    overrides.put(Constants.PROPERTY_MAX_RETRIES, String.valueOf(60));

    return ContextBuilder.newBuilder(provider.getmJcloudsId())
            .credentials(identity, credential)
            .modules(ImmutableSet.of(
                    new SshjSshClientModule(),
                    new Log4JLoggingModule()
            ))
            .overrides(overrides)
            .buildView(ComputeServiceContext.class);
  }

  public Set<NodeMetadata> createNodes(int count)
      throws RunNodesException {
    Set<NodeMetadata> result = Sets.newHashSet();
    result.addAll(mComputeService.createNodesInGroup(mConfig.getGroupName(), count, mTemplate));
    return result;
  }

  static Predicate<ComputeMetadata> createFilterPTestPredicate(final String groupName,
      final String groupTag) {
    return new Predicate<ComputeMetadata>() {
      @Override
      public boolean apply(ComputeMetadata computeMetadata) {
        NodeMetadata nodeMetadata = (NodeMetadata) computeMetadata;
        return nodeMetadata.getStatus() == Status.RUNNING && isPTestHost(nodeMetadata);
      }
      private boolean isPTestHost(NodeMetadata node) {
        String result = "false non-ptest host";
        if(groupName.equalsIgnoreCase(node.getGroup())) {
          result = "true due to group " + groupName;
          return true;
        }
        if(Strings.nullToEmpty(node.getName()).startsWith(groupName)) {
          result = "true due to name " + groupName;
          return true;
        }
        if(node.getTags().contains(groupTag)) {
          result = "true due to tag " + groupName;
          return true;
        }
        LOG.debug("Found node: " + node + ", Result: " + result);
        return false;
      }
    };
  }
  public Set<NodeMetadata> listRunningNodes(){
    Set<NodeMetadata> result = Sets.newHashSet();
    result.addAll(mComputeService.listNodesDetailsMatching(
        createFilterPTestPredicate(mConfig.getGroupName(), mGroupTag)));
    return result;
  }
  public void destroyNode(String nodeId) {
    mComputeService.destroyNode(nodeId);
  }
  public void close() {
    mComputeServiceContext.close();
  }

  public static class CloudComputeConfig {
    public enum CloudComputeProvider {
      AWS("aws-ec2"),
      GCE("google-compute-engine");

      private final String mJcloudsId;

      CloudComputeProvider(String jcloudsId) {
        mJcloudsId = jcloudsId;
      }

      public String getmJcloudsId() {
        return mJcloudsId;
      }
    };

    private final CloudComputeProvider mProvider;

    private String mIdentity;
    private String mCredential;
    private String mInstanceType;
    private String mImageId;
    private String mGroupName;
    private String mSecurityGroup;
    private String mKeyPairName;
    private Map<String, String> mUserMetaData;

    /**
     * JClouds requests on-demand instances when null
     */
    private Float mMaxBid;

    public CloudComputeConfig(CloudComputeProvider provider) {
      mProvider = provider;
    }

    public void setCredentials(String identity, String credential) {
      mIdentity = identity;
      mCredential = credential;
    }

    public void setInstanceType(String instanceType) {
      mInstanceType = instanceType;
    }

    public void setImageId(String imageId) {
      mImageId = imageId;
    }

    public void setGroupName(String groupName) {
      mGroupName = groupName;
    }

    public void setSecurityGroup(String securityGroup) {
      mSecurityGroup = securityGroup;
    }

    public void setMaxBid(Float maxBid) {
      mMaxBid = maxBid;
    }

    public void setmKeyPairName(String keyPairName) {
      mKeyPairName = keyPairName;
    }

    public void setUserMetaData(Map<String, String> userMetaData) {
      mUserMetaData = userMetaData;
    }

    public CloudComputeProvider getmProvider() {
      return mProvider;
    }

    public String getIdentity() {
      return mIdentity;
    }

    public String getCredential() {
      return mCredential;
    }

    public String getInstanceType() {
      return mInstanceType;
    }

    public String getImageId() {
      return mImageId;
    }

    public String getGroupName() {
      return mGroupName;
    }

    public String getSecurityGroup() {
      return mSecurityGroup;
    }

    public Float getMaxBid() {
      return mMaxBid;
    }

    public String getKeyPairName() {
      return mKeyPairName;
    }

    public Map<String, String> getUserMetaData() {
      if (mUserMetaData == null) {
        return ImmutableMap.of();
      }

      return mUserMetaData;
    }
  }

  public static Credentials getCredentialsFromJsonKeyFile(String filename) throws IOException {
    String fileContents = Files.toString(new File(filename), UTF_8);
    Supplier<Credentials> credentialSupplier = new GoogleCredentialsFromJson(fileContents);
    return credentialSupplier.get();
  }
}
