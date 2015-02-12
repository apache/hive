package org.apache.hadoop.hive.metastore;

import org.junit.BeforeClass;

/**
 * Test {@link AbstractTestAuthorizationApiAuthorizer} in embedded mode of metastore
 */
public class TestAuthzApiEmbedAuthorizerInEmbed extends AbstractTestAuthorizationApiAuthorizer {

  @BeforeClass
  public static void setup() throws Exception {
    isRemoteMetastoreMode = false; // embedded metastore mode
    AbstractTestAuthorizationApiAuthorizer.setup();
  }

}
