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

package org.apache.hadoop.hive.metastore.testutils;

import software.amazon.awssdk.regions.Region;

/**
 * AWS Integration tests don't run by default. You need to set up your environment.
 *
 * ACCOUNT_ID={your AWS account ID}
 * HMS_PRINCIPAL_ARN="arn:aws:iam::${ACCOUNT_ID}:{user or role}"
 *
 * REGION=us-east-1
 * ROLE_NAME=hive-s3-vending-test-role
 * export HIVE_IT_AWS_INTEGRATION_TEST_ENABLED=true
 * export HIVE_IT_S3_BUCKET="$BUCKET-$ACCOUNT_ID-$REGION-an"
 * export HIVE_IT_S3_TEST_PATH=hive-test
 * export HIVE_IT_S3_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/$ROLE_NAME"
 * export HIVE_IT_S3_EXTERNAL_ID=hive-s3-vending-test
 * export HIVE_IT_S3_REGION=us-east-1
 *
 * aws s3api create-bucket \
 *   --bucket "${HIVE_IT_S3_BUCKET}" \
 *   --region "${REGION}" \
 *   --bucket-namespace account-regional
 *
 * aws s3api put-bucket-versioning \
 *   --bucket "${HIVE_IT_S3_BUCKET}" \
 *   --versioning-configuration Status=Enabled
 *
 * cat > trust-policy.json <<EOF
 * {
 *   "Version": "2012-10-17",
 *   "Statement": [
 *     {
 *       "Effect": "Allow",
 *       "Principal": {
 *         "AWS": "${HMS_PRINCIPAL_ARN}"
 *       },
 *       "Action": "sts:AssumeRole",
 *       "Condition": {
 *         "StringEquals": {
 *           "sts:ExternalId": "${HIVE_IT_S3_EXTERNAL_ID}"
 *         }
 *       }
 *     }
 *   ]
 * }
 * EOF
 *
 * aws iam create-role \
 *   --role-name "${ROLE_NAME}" \
 *   --assume-role-policy-document file://trust-policy.json
 *
 * cat > role-policy.json <<EOF
 * {
 *   "Version": "2012-10-17",
 *   "Statement": [
 *     {
 *       "Effect": "Allow",
 *       "Action": "s3:GetBucketLocation",
 *       "Resource": "arn:aws:s3:::${HIVE_IT_S3_BUCKET}"
 *     },
 *     {
 *       "Effect": "Allow",
 *       "Action": "s3:ListBucket",
 *       "Resource": "arn:aws:s3:::${HIVE_IT_S3_BUCKET}",
 *       "Condition": {
 *         "StringLike": {
 *           "s3:prefix": [
 *             "${HIVE_IT_S3_TEST_PATH}/*"
 *           ]
 *         }
 *       }
 *     },
 *     {
 *       "Effect": "Allow",
 *       "Action": [
 *         "s3:GetObject",
 *         "s3:GetObjectVersion",
 *         "s3:PutObject",
 *         "s3:DeleteObject"
 *       ],
 *       "Resource": "arn:aws:s3:::${HIVE_IT_S3_BUCKET}/${HIVE_IT_S3_TEST_PATH}/*"
 *     }
 *   ]
 * }
 * EOF
 *
 * aws iam put-role-policy \
 *   --role-name "${ROLE_NAME}" \
 *   --policy-name hive-s3-vending-it-s3 \
 *   --policy-document file://role-policy.json
 */
public record AwsS3IntegrationTestConfig(
    String bucket,
    String basePath,
    String roleArn,
    String externalId,
    String regionId) {
  public static final String ENABLE_ENV = "HIVE_IT_AWS_INTEGRATION_TEST_ENABLED";
  public static final String BUCKET_ENV = "HIVE_IT_S3_BUCKET";
  public static final String PATH_ENV = "HIVE_IT_S3_TEST_PATH";
  public static final String ROLE_ARN_ENV = "HIVE_IT_S3_ROLE_ARN";
  public static final String EXTERNAL_ID_ENV = "HIVE_IT_S3_EXTERNAL_ID";
  public static final String REGION_ENV = "HIVE_IT_S3_REGION";

  public static boolean isConfigured() {
    return Boolean.parseBoolean(System.getenv(ENABLE_ENV))
        && isNonBlank(System.getenv(BUCKET_ENV))
        && isNonBlank(System.getenv(PATH_ENV))
        && isNonBlank(System.getenv(ROLE_ARN_ENV))
        && isNonBlank(System.getenv(REGION_ENV));
  }

  public static AwsS3IntegrationTestConfig fromEnvironment() {
    return new AwsS3IntegrationTestConfig(
        requireSetting(BUCKET_ENV),
        trimSlashes(requireSetting(PATH_ENV)),
        requireSetting(ROLE_ARN_ENV),
        System.getenv(EXTERNAL_ID_ENV),
        requireSetting(REGION_ENV));
  }

  public Region region() {
    return Region.of(regionId);
  }

  private static String requireSetting(String environmentVariable) {
    var value = System.getenv(environmentVariable);
    if (!isNonBlank(value)) {
      throw new IllegalStateException("Missing environment variable: " + environmentVariable);
    }
    return value;
  }

  private static boolean isNonBlank(String value) {
    return value != null && !value.isBlank();
  }

  private static String trimSlashes(String value) {
    var start = 0;
    var end = value.length();
    while (start < end && value.charAt(start) == '/') {
      start++;
    }
    while (start < end && value.charAt(end - 1) == '/') {
      end--;
    }
    return value.substring(start, end);
  }
}
