package org.apache.hadoop.hive.metastore;

import org.junit.BeforeClass;

/**
 * Test {@link TestAuthorizationApiAuthorizer} in embedded mode of metastore
 */
public class TestAuthzApiEmbedAuthorizerInEmbed extends TestAuthorizationApiAuthorizer {

  @BeforeClass
  public static void setup() throws Exception {
    isRemoteMetastoreMode = false; // embedded metastore mode
    TestAuthorizationApiAuthorizer.setup();
  }

}
