package org.apache.hadoop.hive.thrift;

import javax.security.sasl.AuthenticationException;

public class DefaultKrbCustomAuthenticationProviderImpl implements KrbCustomAuthenticationProvider {

 /**
  * This class is a default custom authentication class.
  * It will be called when you set the configuration as
  * hive.server2.authentication=KERBEROS,
  * hive.server2.kerberos.custom.authentication.used=true,
  * and hive.server2.kerberos.custom.authentication.class is unset on hive-site.xml
  **/
  @Override
  public void authenticate(String user, String password) throws AuthenticationException {
      throw new AuthenticationException("Unsupported authentication method on Kerberos environment");
  }
}