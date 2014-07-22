package org.apache.hadoop.hive.metastore;

import org.junit.BeforeClass;

/**
 * Test {@link TestAuthorizationApiAuthorizer} in remote mode of metastore
 */
public class TestAuthzApiEmbedAuthorizerInRemote extends TestAuthorizationApiAuthorizer {

  @BeforeClass
  public static void setup() throws Exception {
    isRemoteMetastoreMode = true; // remote metastore mode
    TestAuthorizationApiAuthorizer.setup();
  }

}
