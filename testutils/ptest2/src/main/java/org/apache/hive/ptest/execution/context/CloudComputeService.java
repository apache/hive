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

import java.util.Set;

import org.jclouds.ContextBuilder;
import org.jclouds.aws.ec2.compute.AWSEC2TemplateOptions;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.ComputeServiceContext;
import org.jclouds.compute.RunNodesException;
import org.jclouds.compute.domain.ComputeMetadata;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.compute.domain.NodeMetadata.Status;
import org.jclouds.compute.domain.Template;
import org.jclouds.ec2.domain.InstanceType;
import org.jclouds.logging.log4j.config.Log4JLoggingModule;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

public class CloudComputeService {
  private final ComputeServiceContext mComputeServiceContext;
  private final ComputeService mComputeService;
  private final String mGroupName;
  private final String mImageId;
  private final String mkeyPair;
  private final String mSecurityGroup;
  private final float mMaxBid;
  public CloudComputeService(String apiKey, String accessKey, String groupName,
      String imageId, String keyPair, String securityGroup, float maxBid) {
    mGroupName = groupName;
    mImageId = imageId;
    mkeyPair = keyPair;
    mSecurityGroup = securityGroup;
    mMaxBid = maxBid;
    mComputeServiceContext = ContextBuilder.newBuilder("aws-ec2")
        .credentials(apiKey, accessKey)
        .modules(ImmutableSet.of(new Log4JLoggingModule()))
        .buildView(ComputeServiceContext.class);
    mComputeService = mComputeServiceContext.getComputeService();
  }
  public Set<NodeMetadata> createNodes(int count)
  throws RunNodesException {
    Set<NodeMetadata> result = Sets.newHashSet();
    Template template = mComputeService.templateBuilder()
        .hardwareId(InstanceType.M1_XLARGE).imageId(mImageId).build();
    template.getOptions().as(AWSEC2TemplateOptions.class).keyPair(mkeyPair)
        .securityGroupIds(mSecurityGroup).blockOnPort(22, 60)
        .spotPrice(mMaxBid);
    result.addAll(mComputeService.createNodesInGroup(mGroupName, count, template));
    return result;
  }
  public Set<NodeMetadata> listRunningNodes(){
    Set<NodeMetadata> result = Sets.newHashSet();
    result.addAll(mComputeService
        .listNodesDetailsMatching(new Predicate<ComputeMetadata>() {
          @Override
          public boolean apply(ComputeMetadata computeMetadata) {
            NodeMetadata nodeMetadata = (NodeMetadata) computeMetadata;
            return nodeMetadata.getStatus() == Status.RUNNING
                && mGroupName.equalsIgnoreCase(nodeMetadata.getGroup());
          }
        }));
    return result;
  }
  public void destroyNode(String nodeId) {
    mComputeService.destroyNode(nodeId);
  }
  public void close() {
    mComputeServiceContext.close();
  }
}
