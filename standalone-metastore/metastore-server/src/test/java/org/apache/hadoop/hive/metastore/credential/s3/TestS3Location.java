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

package org.apache.hadoop.hive.metastore.credential.s3;

import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.net.URI;
import java.net.URISyntaxException;

@Category(MetastoreUnitTest.class)
public class TestS3Location {
  @Test
  public void test() {
    var location = S3Location.create("aws", URI.create("s3://bucket/warehouse/tbl")).orElseThrow();
    Assert.assertEquals("arn:aws:s3:::bucket", location.getBucketArn().toString());
    Assert.assertEquals("arn:aws:s3:::bucket/warehouse/tbl/*", location.getWildCardArn().toString());
    Assert.assertEquals("warehouse/tbl/*", location.getWildCardPath());
  }

  @Test
  public void testTrailingSlash() {
    var location = S3Location.create("aws", URI.create("s3://bucket/warehouse/tbl/")).orElseThrow();
    Assert.assertEquals("arn:aws:s3:::bucket", location.getBucketArn().toString());
    Assert.assertEquals("arn:aws:s3:::bucket/warehouse/tbl/*", location.getWildCardArn().toString());
    Assert.assertEquals("warehouse/tbl/*", location.getWildCardPath());
  }

  @Test
  public void testRoot() {
    var location = S3Location.create("aws", URI.create("s3://bucket")).orElseThrow();
    Assert.assertEquals("arn:aws:s3:::bucket", location.getBucketArn().toString());
    Assert.assertEquals("arn:aws:s3:::bucket/*", location.getWildCardArn().toString());
    Assert.assertEquals("*", location.getWildCardPath());
  }

  @Test
  public void testRootWithTrailingSlash() {
    var location = S3Location.create("aws", URI.create("s3://bucket/")).orElseThrow();
    Assert.assertEquals("arn:aws:s3:::bucket", location.getBucketArn().toString());
    Assert.assertEquals("arn:aws:s3:::bucket/*", location.getWildCardArn().toString());
    Assert.assertEquals("*", location.getWildCardPath());
  }

  @Test
  public void testS3A() {
    var location = S3Location.create("aws", URI.create("s3a://bucket/warehouse/tbl")).orElseThrow();
    Assert.assertEquals("arn:aws:s3:::bucket", location.getBucketArn().toString());
    Assert.assertEquals("arn:aws:s3:::bucket/warehouse/tbl/*", location.getWildCardArn().toString());
    Assert.assertEquals("warehouse/tbl/*", location.getWildCardPath());
  }

  @Test
  public void testS3N() {
    var location = S3Location.create("aws", URI.create("s3n://bucket/warehouse/tbl")).orElseThrow();
    Assert.assertEquals("arn:aws:s3:::bucket", location.getBucketArn().toString());
    Assert.assertEquals("arn:aws:s3:::bucket/warehouse/tbl/*", location.getWildCardArn().toString());
    Assert.assertEquals("warehouse/tbl/*", location.getWildCardPath());
  }

  @Test
  public void testPartition() {
    var location = S3Location.create("aws-us-gov", URI.create("s3://bucket/warehouse/tbl")).orElseThrow();
    Assert.assertEquals("arn:aws-us-gov:s3:::bucket", location.getBucketArn().toString());
    Assert.assertEquals("arn:aws-us-gov:s3:::bucket/warehouse/tbl/*", location.getWildCardArn().toString());
    Assert.assertEquals("warehouse/tbl/*", location.getWildCardPath());
  }

  @Test
  public void testMatchesArnPrefix() {
    var location = S3Location.create("aws", URI.create("s3://bucket/warehouse/tbl")).orElseThrow();

    Assert.assertTrue(location.matches("arn:aws:s3:::bucket"));
    Assert.assertTrue(location.matches("arn:aws:s3:::bucket/warehouse/"));
    Assert.assertFalse(location.matches("arn:aws:s3:::bucket/curated/"));
  }

  @Test
  public void testMatchesBucketPrefix() {
    var location = S3Location.create("aws", URI.create("s3://bucket/warehouse/tbl")).orElseThrow();

    Assert.assertTrue(location.matches("bucket"));
    Assert.assertTrue(location.matches("bucket/warehouse/"));
    Assert.assertFalse(location.matches("bucket/curated/"));
  }

  @Test
  public void testUnsupportedPaths() {
    Assert.assertTrue(S3Location.create("aws", URI.create("/bucket/warehouse/tbl")).isEmpty());
    Assert.assertTrue(S3Location.create("aws", URI.create("s3b://bucket/warehouse/tbl")).isEmpty());
    Assert.assertTrue(S3Location.create("aws", URI.create("s3:///warehouse/tbl")).isEmpty());
    Assert.assertTrue(S3Location.create("aws", URI.create("s3:bucket")).isEmpty());
  }
}
