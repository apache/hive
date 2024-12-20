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
package org.apache.hadoop.hive.common.auth;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManagerFactory;

import com.google.common.base.Splitter;
import com.google.common.collect.Sets;
import org.apache.thrift.transport.TSSLTransportFactory;
import org.apache.thrift.transport.TSSLTransportFactory.TSSLTransportParameters;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class helps in some aspects of authentication. It creates the proper Thrift classes for the
 * given configuration as well as helps with authenticating requests.
 */
public class HiveAuthUtils {
  private static final Logger LOG = LoggerFactory.getLogger(HiveAuthUtils.class);

  /**
   * Configure the provided T transport's max message size.
   * @param transport Transport to configure maxMessage for
   * @param maxMessageSize Maximum allowed message size in bytes, less than or equal to 0 means use the Thrift library
   *                       default.
   * @return The passed in T transport configured with desired max message size. The same object passed in is returned.
   */
  public static <T extends TTransport> T configureThriftMaxMessageSize(T transport, int maxMessageSize) {
    if (maxMessageSize > 0) {
      if (transport.getConfiguration() == null) {
        LOG.warn("TTransport {} is returning a null Configuration, Thrift max message size is not getting configured",
            transport.getClass().getName());
        return transport;
      }
      transport.getConfiguration().setMaxMessageSize(maxMessageSize);
    }
    return transport;
  }

  /**
   * Create a TSocket for the provided host and port with specified loginTimeout. Thrift maxMessageSize
   * will default to Thrift library default.
   * @param host Host to connect to.
   * @param port Port to connect to.
   * @param loginTimeout Socket timeout (0 means no timeout).
   * @return TTransport TSocket for host/port.
   */
  public static TTransport getSocketTransport(String host, int port, int loginTimeout) throws TTransportException {
    return getSocketTransport(host, port, loginTimeout, /* maxMessageSize */ -1);
  }

  /**
   * Create a TSocket for the provided host and port with specified loginTimeout and maxMessageSize.
   * will default to Thrift library default.
   * @param host Host to connect to.
   * @param port Port to connect to.
   * @param loginTimeout Socket timeout (0 means no timeout).
   * @param maxMessageSize Size in bytes for max allowable Thrift message size, less than or equal to 0
   *                       results in using the Thrift library default.
   * @return TTransport TSocket for host/port
   */
  public static TTransport getSocketTransport(String host, int port, int loginTimeout, int maxMessageSize)
      throws TTransportException {
    TSocket tSocket = new TSocket(host, port, loginTimeout);
    return configureThriftMaxMessageSize(tSocket, maxMessageSize);
  }

  public static TTransport getSSLSocket(String host, int port, int loginTimeout, TSSLTransportParameters params,
      int maxMessageSize) throws TTransportException {
    // The underlying SSLSocket object is bound to host:port with the given SO_TIMEOUT and
    // SSLContext created with the given params
    TSocket tSSLSocket = null;
    if (params != null) {
      tSSLSocket = TSSLTransportFactory.getClientSocket(host, port, loginTimeout, params);
    } else {
      tSSLSocket = TSSLTransportFactory.getClientSocket(host, port, loginTimeout);
    }
    return getSSLSocketWithHttps(tSSLSocket, maxMessageSize);
  }

  public static TTransport getSSLSocket(String host, int port, int loginTimeout, String trustStorePath,
      String trustStorePassWord, String trustStoreType, String trustStoreAlgorithm) throws TTransportException {
    return getSSLSocket(host, port, loginTimeout, trustStorePath, trustStorePassWord, trustStoreType,
        trustStoreAlgorithm, /* maxMessageSize */ -1);
  }

  public static TTransport getSSLSocket(String host, int port, int loginTimeout) throws TTransportException {
    return getSSLSocket(host, port, loginTimeout, /* maxMessageSize */ -1);
  }

  public static TTransport getSSLSocket(String host, int port, int loginTimeout, int maxMessageSize)
      throws TTransportException {
    return getSSLSocket(host, port, loginTimeout, /* params */ null, maxMessageSize);
  }

  public static TTransport getSSLSocket(String host, int port, int loginTimeout, String trustStorePath,
      String trustStorePassWord, String trustStoreType, String trustStoreAlgorithm, int maxMessageSize)
      throws TTransportException {
    TSSLTransportParameters params = new TSSLTransportParameters();
    String tStoreType = trustStoreType.isEmpty()? KeyStore.getDefaultType() : trustStoreType;
    String tStoreAlgorithm = trustStoreAlgorithm.isEmpty()?
            TrustManagerFactory.getDefaultAlgorithm() : trustStoreAlgorithm;
    params.setTrustStore(trustStorePath, trustStorePassWord, tStoreAlgorithm, tStoreType);
    params.requireClientAuth(true);
    return getSSLSocket(host, port, loginTimeout, params, maxMessageSize);
  }

  // Using endpoint identification algorithm as HTTPS enables us to do
  // CNAMEs/subjectAltName verification
  private static TSocket getSSLSocketWithHttps(TSocket tSSLSocket, int maxMessageSize)
      throws TTransportException {
    SSLSocket sslSocket = (SSLSocket) tSSLSocket.getSocket();
    SSLParameters sslParams = sslSocket.getSSLParameters();
    if (sslSocket.getLocalAddress().getHostAddress().equals("127.0.0.1")) {
      sslParams.setEndpointIdentificationAlgorithm(null);
    } else {
      sslParams.setEndpointIdentificationAlgorithm("HTTPS");
    }
    sslSocket.setSSLParameters(sslParams);
    TSocket tSocket = new TSocket(sslSocket);
    return configureThriftMaxMessageSize(tSocket, maxMessageSize);
  }

  public static TServerSocket getServerSocket(String hiveHost, int portNum)
      throws TTransportException {
    InetSocketAddress serverAddress;
    if (hiveHost == null || hiveHost.isEmpty()) {
      // Wildcard bind
      serverAddress = new InetSocketAddress(portNum);
    } else {
      serverAddress = new InetSocketAddress(hiveHost, portNum);
    }
    return new TServerSocket(serverAddress);
  }

  public static TServerSocket getServerSSLSocket(String hiveHost, int portNum, String keyStorePath,
      String keyStorePassWord, String keyStoreType, String keyStoreAlgorithm,
      List<String> sslVersionBlacklist, String includeCipherSuites) throws TTransportException,
      UnknownHostException {

    TSSLTransportFactory.TSSLTransportParameters params = null;
    if (!includeCipherSuites.trim().isEmpty()) {
      Set<String> includeCS = Sets.newHashSet(
              Splitter.on(":").trimResults().omitEmptyStrings().split(includeCipherSuites.trim()));
      int eSize = includeCS.size();
      if (eSize > 0) {
        params = new TSSLTransportFactory.TSSLTransportParameters("TLS", includeCS.toArray(new String[eSize]));
      }
    }
    if (params == null) {
      params = new TSSLTransportFactory.TSSLTransportParameters();
    }
    String kStoreType = keyStoreType.isEmpty()? KeyStore.getDefaultType() : keyStoreType;
    String kStoreAlgorithm = keyStoreAlgorithm.isEmpty()?
            KeyManagerFactory.getDefaultAlgorithm() : keyStoreAlgorithm;
    params.setKeyStore(keyStorePath, keyStorePassWord,
        kStoreAlgorithm, kStoreType);
    InetSocketAddress serverAddress;
    if (hiveHost == null || hiveHost.isEmpty()) {
      // Wildcard bind
      serverAddress = new InetSocketAddress(portNum);
    } else {
      serverAddress = new InetSocketAddress(hiveHost, portNum);
    }
    TServerSocket thriftServerSocket =
        TSSLTransportFactory.getServerSocket(portNum, 0, serverAddress.getAddress(), params);
    if (thriftServerSocket.getServerSocket() instanceof SSLServerSocket) {
      List<String> sslVersionBlacklistLocal = new ArrayList<String>();
      for (String sslVersion : sslVersionBlacklist) {
        sslVersionBlacklistLocal.add(sslVersion.trim().toLowerCase());
      }
      SSLServerSocket sslServerSocket = (SSLServerSocket) thriftServerSocket.getServerSocket();
      List<String> enabledProtocols = new ArrayList<String>();
      for (String protocol : sslServerSocket.getEnabledProtocols()) {
        if (sslVersionBlacklistLocal.contains(protocol.toLowerCase())) {
          LOG.debug("Disabling SSL Protocol: " + protocol);
        } else {
          enabledProtocols.add(protocol);
        }
      }
      sslServerSocket.setEnabledProtocols(enabledProtocols.toArray(new String[0]));
      LOG.info("SSL Server Socket Enabled Protocols: "
          + Arrays.toString(sslServerSocket.getEnabledProtocols()));
    }
    return thriftServerSocket;
  }
}
