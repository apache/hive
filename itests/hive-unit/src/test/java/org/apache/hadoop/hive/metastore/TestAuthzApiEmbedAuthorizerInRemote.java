package org.apache.hadoop.hive.metastore;

import org.junit.BeforeClass;

/**
 * Test {@link AbstractTestAuthorizationApiAuthorizer} in remote mode of metastore
 */
public class TestAuthzApiEmbedAuthorizerInRemote extends AbstractTestAuthorizationApiAuthorizer {

  @BeforeClass
  public static void setup() throws Exception {
    isRemoteMetastoreMode = true; // remote metastore mode
    AbstractTestAuthorizationApiAuthorizer.setup();
  }

}
