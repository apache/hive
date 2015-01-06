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

import java.util.Collections;
import java.util.Properties;
import java.util.Map;
import java.util.Set;

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
import org.jclouds.logging.log4j.config.Log4JLoggingModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

public class CloudComputeService {
  private static final Logger LOG = LoggerFactory
      .getLogger(CloudComputeService.class);
  private final ComputeServiceContext mComputeServiceContext;
  private final ComputeService mComputeService;
  private final String mInstanceType;
  private final String mGroupName;
  private final String mGroupTag;
  private final String mImageId;
  private final String mkeyPair;
  private final String mSecurityGroup;
  private final Map<String, String> mUserMetadata;
  /**
   * JClouds requests on-demand instances when null
   */
  private final Float mMaxBid;
  public CloudComputeService(String apiKey, String accessKey, String instanceType, String groupName,
      String imageId, String keyPair, String securityGroup, Float maxBid, Map<String,String> userMetadata) {
    mInstanceType = instanceType;
    mGroupName = groupName;
    mImageId = imageId;
    mkeyPair = keyPair;
    mSecurityGroup = securityGroup;
    mMaxBid = maxBid;
    mGroupTag = "group=" + mGroupName;
    mUserMetadata = userMetadata;
    Properties overrides = new Properties();
    overrides.put(ComputeServiceProperties.POLL_INITIAL_PERIOD, String.valueOf(60L * 1000L));
    overrides.put(ComputeServiceProperties.POLL_MAX_PERIOD, String.valueOf(600L * 1000L));
    overrides.put(Constants.PROPERTY_MAX_RETRIES, String.valueOf(60));
    mComputeServiceContext = ContextBuilder.newBuilder("aws-ec2")
        .credentials(apiKey, accessKey)
        .modules(ImmutableSet.of(new Log4JLoggingModule()))
        .overrides(overrides)
        .buildView(ComputeServiceContext.class);
    mComputeService = mComputeServiceContext.getComputeService();
  }
  public Set<NodeMetadata> createNodes(int count)
      throws RunNodesException {
    Set<NodeMetadata> result = Sets.newHashSet();
    Template template = mComputeService.templateBuilder()
        .hardwareId(mInstanceType).imageId(mImageId).build();
    template.getOptions().as(AWSEC2TemplateOptions.class).keyPair(mkeyPair)
    .securityGroupIds(mSecurityGroup).blockOnPort(22, 60)
    .spotPrice(mMaxBid).tags(Collections.singletonList(mGroupTag))
    .userMetadata(mUserMetadata);
    result.addAll(mComputeService.createNodesInGroup(mGroupName, count, template));
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
        createFilterPTestPredicate(mGroupName, mGroupTag)));
    return result;
  }
  public void destroyNode(String nodeId) {
    mComputeService.destroyNode(nodeId);
  }
  public void close() {
    mComputeServiceContext.close();
  }
}
