package org.apache.hadoop.hive.thrift;

import javax.security.sasl.AuthenticationException;

public interface KrbCustomAuthenticationProvider {
  /**
   * The Authenticate method is called by the thrift (HiveServer2, HiveMetastore) authentication layer
   * to authenticate users for their requests (Server side).
   * It has the same pattern with PasswordAuthenticationProvider.
   * If a user is to be granted, return nothing/throw nothing.
   * When a user is to be disallowed, throw an appropriate {@link AuthenticationException}.
   *
   * For an example implementation, see {@link LdapAuthenticationProviderImpl}.
   *
   * @param user - The username received over the connection request
   * @param password - The password received over the connection request
   * @throws AuthenticationException - When a user is found to be
   * invalid by the implementation
   */
   void authenticate(String user, String password) throws AuthenticationException;
}
