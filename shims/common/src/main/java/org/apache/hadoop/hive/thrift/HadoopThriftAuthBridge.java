/**
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
package org.apache.hadoop.hive.thrift;

import static org.apache.hadoop.fs.CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.Locale;
import java.util.Map;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.RealmCallback;
import javax.security.sasl.RealmChoiceCallback;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.thrift.client.TUGIAssumingTransport;
import org.apache.hadoop.hive.thrift.DelegationTokenIdentifier;
import org.apache.hadoop.hive.thrift.DelegationTokenSecretManager;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.SaslRpcServer.AuthMethod;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSaslClientTransport;
import org.apache.thrift.transport.TSaslServerTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;

/**
 * Functions that bridge Thrift's SASL transports to Hadoop's
 * SASL callback handlers and authentication classes.
 * HIVE-11378 This class is not directly used anymore.  It now exists only as a shell to be
 * extended by HadoopThriftAuthBridge23 in 0.23 shims.  I have made it abstract
 * to avoid maintenance errors.
 */
public abstract class HadoopThriftAuthBridge {
  private static final Logger LOG = LoggerFactory.getLogger(HadoopThriftAuthBridge.class);

  public Client createClient() {
    return new Client();
  }

  public Client createClientWithConf(String authMethod) {
    UserGroupInformation ugi;
    try {
      ugi = UserGroupInformation.getLoginUser();
    } catch(IOException e) {
      throw new IllegalStateException("Unable to get current login user: " + e, e);
    }
    if (loginUserHasCurrentAuthMethod(ugi, authMethod)) {
      LOG.debug("Not setting UGI conf as passed-in authMethod of " + authMethod + " = current.");
      return new Client();
    } else {
      LOG.debug("Setting UGI conf as passed-in authMethod of " + authMethod + " != current.");
      Configuration conf = new Configuration();
      conf.set(HADOOP_SECURITY_AUTHENTICATION, authMethod);
      UserGroupInformation.setConfiguration(conf);
      return new Client();
    }
  }

  public Server createServer(String keytabFile, String principalConf) throws TTransportException {
    return new Server(keytabFile, principalConf);
  }


  public String getServerPrincipal(String principalConfig, String host)
      throws IOException {
    String serverPrincipal = SecurityUtil.getServerPrincipal(principalConfig, host);
    String names[] = SaslRpcServer.splitKerberosName(serverPrincipal);
    if (names.length != 3) {
      throw new IOException(
          "Kerberos principal name does NOT have the expected hostname part: "
              + serverPrincipal);
    }
    return serverPrincipal;
  }


  public UserGroupInformation getCurrentUGIWithConf(String authMethod)
      throws IOException {
    UserGroupInformation ugi;
    try {
      ugi = UserGroupInformation.getCurrentUser();
    } catch(IOException e) {
      throw new IllegalStateException("Unable to get current user: " + e, e);
    }
    if (loginUserHasCurrentAuthMethod(ugi, authMethod)) {
      LOG.debug("Not setting UGI conf as passed-in authMethod of " + authMethod + " = current.");
      return ugi;
    } else {
      LOG.debug("Setting UGI conf as passed-in authMethod of " + authMethod + " != current.");
      Configuration conf = new Configuration();
      conf.set(HADOOP_SECURITY_AUTHENTICATION, authMethod);
      UserGroupInformation.setConfiguration(conf);
      return UserGroupInformation.getCurrentUser();
    }
  }

  /**
   * Return true if the current login user is already using the given authMethod.
   *
   * Used above to ensure we do not create a new Configuration object and as such
   * lose other settings such as the cluster to which the JVM is connected. Required
   * for oozie since it does not have a core-site.xml see HIVE-7682
   */
  private boolean loginUserHasCurrentAuthMethod(UserGroupInformation ugi, String sAuthMethod) {
    AuthenticationMethod authMethod;
    try {
      // based on SecurityUtil.getAuthenticationMethod()
      authMethod = Enum.valueOf(AuthenticationMethod.class, sAuthMethod.toUpperCase(Locale.ENGLISH));
    } catch (IllegalArgumentException iae) {
      throw new IllegalArgumentException("Invalid attribute value for " +
          HADOOP_SECURITY_AUTHENTICATION + " of " + sAuthMethod, iae);
    }
    LOG.debug("Current authMethod = " + ugi.getAuthenticationMethod());
    return ugi.getAuthenticationMethod().equals(authMethod);
  }


  /**
   * Read and return Hadoop SASL configuration which can be configured using
   * "hadoop.rpc.protection"
   * @param conf
   * @return Hadoop SASL configuration
   */

  public abstract Map<String, String> getHadoopSaslProperties(Configuration conf);

  public static class Client {
    /**
     * Create a client-side SASL transport that wraps an underlying transport.
     *
     * @param method The authentication method to use. Currently only KERBEROS is
     *               supported.
     * @param serverPrincipal The Kerberos principal of the target server.
     * @param underlyingTransport The underlying transport mechanism, usually a TSocket.
     * @param saslProps the sasl properties to create the client with
     */


    public TTransport createClientTransport(
        String principalConfig, String host,
        String methodStr, String tokenStrForm, TTransport underlyingTransport,
        Map<String, String> saslProps) throws IOException {
      AuthMethod method = AuthMethod.valueOf(AuthMethod.class, methodStr);

      TTransport saslTransport = null;
      switch (method) {
      case DIGEST:
        Token<DelegationTokenIdentifier> t= new Token<DelegationTokenIdentifier>();
        t.decodeFromUrlString(tokenStrForm);
        saslTransport = new TSaslClientTransport(
            method.getMechanismName(),
            null,
            null, SaslRpcServer.SASL_DEFAULT_REALM,
            saslProps, new SaslClientCallbackHandler(t),
            underlyingTransport);
        return new TUGIAssumingTransport(saslTransport, UserGroupInformation.getCurrentUser());

      case KERBEROS:
        String serverPrincipal = SecurityUtil.getServerPrincipal(principalConfig, host);
        String names[] = SaslRpcServer.splitKerberosName(serverPrincipal);
        if (names.length != 3) {
          throw new IOException(
              "Kerberos principal name does NOT have the expected hostname part: "
                  + serverPrincipal);
        }
        try {
          saslTransport = new TSaslClientTransport(
              method.getMechanismName(),
              null,
              names[0], names[1],
              saslProps, null,
              underlyingTransport);
          return new TUGIAssumingTransport(saslTransport, UserGroupInformation.getCurrentUser());
        } catch (SaslException se) {
          throw new IOException("Could not instantiate SASL transport", se);
        }

      default:
        throw new IOException("Unsupported authentication method: " + method);
      }
    }
    private static class SaslClientCallbackHandler implements CallbackHandler {
      private final String userName;
      private final char[] userPassword;

      public SaslClientCallbackHandler(Token<? extends TokenIdentifier> token) {
        this.userName = encodeIdentifier(token.getIdentifier());
        this.userPassword = encodePassword(token.getPassword());
      }


      @Override
      public void handle(Callback[] callbacks)
          throws UnsupportedCallbackException {
        NameCallback nc = null;
        PasswordCallback pc = null;
        RealmCallback rc = null;
        for (Callback callback : callbacks) {
          if (callback instanceof RealmChoiceCallback) {
            continue;
          } else if (callback instanceof NameCallback) {
            nc = (NameCallback) callback;
          } else if (callback instanceof PasswordCallback) {
            pc = (PasswordCallback) callback;
          } else if (callback instanceof RealmCallback) {
            rc = (RealmCallback) callback;
          } else {
            throw new UnsupportedCallbackException(callback,
                "Unrecognized SASL client callback");
          }
        }
        if (nc != null) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("SASL client callback: setting username: " + userName);
          }
          nc.setName(userName);
        }
        if (pc != null) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("SASL client callback: setting userPassword");
          }
          pc.setPassword(userPassword);
        }
        if (rc != null) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("SASL client callback: setting realm: "
                + rc.getDefaultText());
          }
          rc.setText(rc.getDefaultText());
        }
      }

      static String encodeIdentifier(byte[] identifier) {
        return new String(Base64.encodeBase64(identifier));
      }

      static char[] encodePassword(byte[] password) {
        return new String(Base64.encodeBase64(password)).toCharArray();
      }
    }
  }

  public static class Server {
    public enum ServerMode {
      HIVESERVER2, METASTORE
    };

    protected final UserGroupInformation realUgi;
    protected DelegationTokenSecretManager secretManager;

    public Server() throws TTransportException {
      try {
        realUgi = UserGroupInformation.getCurrentUser();
      } catch (IOException ioe) {
        throw new TTransportException(ioe);
      }
    }
    /**
     * Create a server with a kerberos keytab/principal.
     */
    protected Server(String keytabFile, String principalConf)
        throws TTransportException {
      if (keytabFile == null || keytabFile.isEmpty()) {
        throw new TTransportException("No keytab specified");
      }
      if (principalConf == null || principalConf.isEmpty()) {
        throw new TTransportException("No principal specified");
      }

      // Login from the keytab
      String kerberosName;
      try {
        kerberosName =
            SecurityUtil.getServerPrincipal(principalConf, "0.0.0.0");
        UserGroupInformation.loginUserFromKeytab(
            kerberosName, keytabFile);
        realUgi = UserGroupInformation.getLoginUser();
        assert realUgi.isFromKeytab();
      } catch (IOException ioe) {
        throw new TTransportException(ioe);
      }
    }

    public void setSecretManager(DelegationTokenSecretManager secretManager) {
      this.secretManager = secretManager;
    }

    /**
     * Create a TTransportFactory that, upon connection of a client socket,
     * negotiates a Kerberized SASL transport. The resulting TTransportFactory
     * can be passed as both the input and output transport factory when
     * instantiating a TThreadPoolServer, for example.
     *
     * @param saslProps Map of SASL properties
     */

    public TTransportFactory createTransportFactory(Map<String, String> saslProps)
        throws TTransportException {

      TSaslServerTransport.Factory transFactory = createSaslServerTransportFactory(saslProps);

      return new TUGIAssumingTransportFactory(transFactory, realUgi);
    }

    /**
     * Create a TSaslServerTransport.Factory that, upon connection of a client
     * socket, negotiates a Kerberized SASL transport.
     *
     * @param saslProps Map of SASL properties
     */
    public TSaslServerTransport.Factory createSaslServerTransportFactory(
        Map<String, String> saslProps) throws TTransportException {
      // Parse out the kerberos principal, host, realm.
      String kerberosName = realUgi.getUserName();
      final String names[] = SaslRpcServer.splitKerberosName(kerberosName);
      if (names.length != 3) {
        throw new TTransportException("Kerberos principal should have 3 parts: " + kerberosName);
      }

      TSaslServerTransport.Factory transFactory = new TSaslServerTransport.Factory();
      transFactory.addServerDefinition(
          AuthMethod.KERBEROS.getMechanismName(),
          names[0], names[1],  // two parts of kerberos principal
          saslProps,
          new SaslRpcServer.SaslGssCallbackHandler());
      transFactory.addServerDefinition(AuthMethod.DIGEST.getMechanismName(),
          null, SaslRpcServer.SASL_DEFAULT_REALM,
          saslProps, new SaslDigestCallbackHandler(secretManager));

      return transFactory;
    }

    /**
     * Wrap a TTransportFactory in such a way that, before processing any RPC, it
     * assumes the UserGroupInformation of the user authenticated by
     * the SASL transport.
     */
    public TTransportFactory wrapTransportFactory(TTransportFactory transFactory) {
      return new TUGIAssumingTransportFactory(transFactory, realUgi);
    }

    /**
     * Wrap a TProcessor in such a way that, before processing any RPC, it
     * assumes the UserGroupInformation of the user authenticated by
     * the SASL transport.
     */

    public TProcessor wrapProcessor(TProcessor processor) {
      return new TUGIAssumingProcessor(processor, secretManager, true);
    }

    /**
     * Wrap a TProcessor to capture the client information like connecting userid, ip etc
     */

    public TProcessor wrapNonAssumingProcessor(TProcessor processor) {
      return new TUGIAssumingProcessor(processor, secretManager, false);
    }

    final static ThreadLocal<InetAddress> remoteAddress =
        new ThreadLocal<InetAddress>() {

      @Override
      protected InetAddress initialValue() {
        return null;
      }
    };

    public InetAddress getRemoteAddress() {
      return remoteAddress.get();
    }

    final static ThreadLocal<AuthenticationMethod> authenticationMethod =
        new ThreadLocal<AuthenticationMethod>() {

      @Override
      protected AuthenticationMethod initialValue() {
        return AuthenticationMethod.TOKEN;
      }
    };

    private static ThreadLocal<String> remoteUser = new ThreadLocal<String> () {

      @Override
      protected String initialValue() {
        return null;
      }
    };


    public String getRemoteUser() {
      return remoteUser.get();
    }

    /** CallbackHandler for SASL DIGEST-MD5 mechanism */
    // This code is pretty much completely based on Hadoop's
    // SaslRpcServer.SaslDigestCallbackHandler - the only reason we could not
    // use that Hadoop class as-is was because it needs a Server.Connection object
    // which is relevant in hadoop rpc but not here in the metastore - so the
    // code below does not deal with the Connection Server.object.
    static class SaslDigestCallbackHandler implements CallbackHandler {
      private final DelegationTokenSecretManager secretManager;

      public SaslDigestCallbackHandler(
          DelegationTokenSecretManager secretManager) {
        this.secretManager = secretManager;
      }

      private char[] getPassword(DelegationTokenIdentifier tokenid) throws InvalidToken {
        return encodePassword(secretManager.retrievePassword(tokenid));
      }

      private char[] encodePassword(byte[] password) {
        return new String(Base64.encodeBase64(password)).toCharArray();
      }
      /** {@inheritDoc} */

      @Override
      public void handle(Callback[] callbacks) throws InvalidToken,
      UnsupportedCallbackException {
        NameCallback nc = null;
        PasswordCallback pc = null;
        AuthorizeCallback ac = null;
        for (Callback callback : callbacks) {
          if (callback instanceof AuthorizeCallback) {
            ac = (AuthorizeCallback) callback;
          } else if (callback instanceof NameCallback) {
            nc = (NameCallback) callback;
          } else if (callback instanceof PasswordCallback) {
            pc = (PasswordCallback) callback;
          } else if (callback instanceof RealmCallback) {
            continue; // realm is ignored
          } else {
            throw new UnsupportedCallbackException(callback,
                "Unrecognized SASL DIGEST-MD5 Callback");
          }
        }
        if (pc != null) {
          DelegationTokenIdentifier tokenIdentifier = SaslRpcServer.
              getIdentifier(nc.getDefaultName(), secretManager);
          char[] password = getPassword(tokenIdentifier);

          if (LOG.isDebugEnabled()) {
            LOG.debug("SASL server DIGEST-MD5 callback: setting password "
                + "for client: " + tokenIdentifier.getUser());
          }
          pc.setPassword(password);
        }
        if (ac != null) {
          String authid = ac.getAuthenticationID();
          String authzid = ac.getAuthorizationID();
          if (authid.equals(authzid)) {
            ac.setAuthorized(true);
          } else {
            ac.setAuthorized(false);
          }
          if (ac.isAuthorized()) {
            if (LOG.isDebugEnabled()) {
              String username =
                  SaslRpcServer.getIdentifier(authzid, secretManager).getUser().getUserName();
              LOG.debug("SASL server DIGEST-MD5 callback: setting "
                  + "canonicalized client ID: " + username);
            }
            ac.setAuthorizedID(authzid);
          }
        }
      }
    }

    /**
     * Processor that pulls the SaslServer object out of the transport, and
     * assumes the remote user's UGI before calling through to the original
     * processor.
     *
     * This is used on the server side to set the UGI for each specific call.
     */
    protected class TUGIAssumingProcessor implements TProcessor {
      final TProcessor wrapped;
      DelegationTokenSecretManager secretManager;
      boolean useProxy;
      TUGIAssumingProcessor(TProcessor wrapped, DelegationTokenSecretManager secretManager,
          boolean useProxy) {
        this.wrapped = wrapped;
        this.secretManager = secretManager;
        this.useProxy = useProxy;
      }


      @Override
      public boolean process(final TProtocol inProt, final TProtocol outProt) throws TException {
        TTransport trans = inProt.getTransport();
        if (!(trans instanceof TSaslServerTransport)) {
          throw new TException("Unexpected non-SASL transport " + trans.getClass());
        }
        TSaslServerTransport saslTrans = (TSaslServerTransport)trans;
        SaslServer saslServer = saslTrans.getSaslServer();
        String authId = saslServer.getAuthorizationID();
        authenticationMethod.set(AuthenticationMethod.KERBEROS);
        LOG.debug("AUTH ID ======>" + authId);
        String endUser = authId;

        if(saslServer.getMechanismName().equals("DIGEST-MD5")) {
          try {
            TokenIdentifier tokenId = SaslRpcServer.getIdentifier(authId,
                secretManager);
            endUser = tokenId.getUser().getUserName();
            authenticationMethod.set(AuthenticationMethod.TOKEN);
          } catch (InvalidToken e) {
            throw new TException(e.getMessage());
          }
        }
        Socket socket = ((TSocket)(saslTrans.getUnderlyingTransport())).getSocket();
        remoteAddress.set(socket.getInetAddress());
        UserGroupInformation clientUgi = null;
        try {
          if (useProxy) {
            clientUgi = UserGroupInformation.createProxyUser(
                endUser, UserGroupInformation.getLoginUser());
            remoteUser.set(clientUgi.getShortUserName());
            LOG.debug("Set remoteUser :" + remoteUser.get());
            return clientUgi.doAs(new PrivilegedExceptionAction<Boolean>() {

              @Override
              public Boolean run() {
                try {
                  return wrapped.process(inProt, outProt);
                } catch (TException te) {
                  throw new RuntimeException(te);
                }
              }
            });
          } else {
            // use the short user name for the request
            UserGroupInformation endUserUgi = UserGroupInformation.createRemoteUser(endUser);
            remoteUser.set(endUserUgi.getShortUserName());
            LOG.debug("Set remoteUser :" + remoteUser.get() + ", from endUser :" + endUser);
            return wrapped.process(inProt, outProt);
          }
        } catch (RuntimeException rte) {
          if (rte.getCause() instanceof TException) {
            throw (TException)rte.getCause();
          }
          throw rte;
        } catch (InterruptedException ie) {
          throw new RuntimeException(ie); // unexpected!
        } catch (IOException ioe) {
          throw new RuntimeException(ioe); // unexpected!
        }
        finally {
          if (clientUgi != null) {
            try { FileSystem.closeAllForUGI(clientUgi); }
            catch(IOException exception) {
              LOG.error("Could not clean up file-system handles for UGI: " + clientUgi, exception);
            }
          }
        }
      }
    }

    /**
     * A TransportFactory that wraps another one, but assumes a specified UGI
     * before calling through.
     *
     * This is used on the server side to assume the server's Principal when accepting
     * clients.
     */
    static class TUGIAssumingTransportFactory extends TTransportFactory {
      private final UserGroupInformation ugi;
      private final TTransportFactory wrapped;

      public TUGIAssumingTransportFactory(TTransportFactory wrapped, UserGroupInformation ugi) {
        assert wrapped != null;
        assert ugi != null;
        this.wrapped = wrapped;
        this.ugi = ugi;
      }


      @Override
      public TTransport getTransport(final TTransport trans) {
        return ugi.doAs(new PrivilegedAction<TTransport>() {
          @Override
          public TTransport run() {
            return wrapped.getTransport(trans);
          }
        });
      }
    }
  }
}
