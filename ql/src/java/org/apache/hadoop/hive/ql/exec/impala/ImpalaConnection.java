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
package org.apache.hadoop.hive.ql.exec.impala;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.thrift.TCustomSocket;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.impala.thrift.ImpalaHiveServer2Service;

import java.net.InetSocketAddress;

/**
 * Encapsulates a connection to an Impala coordinator.
 */
class ImpalaConnection {
    static private final TBinaryProtocol.Factory protocolFactory = new TBinaryProtocol.Factory();
    private final InetSocketAddress socketAddress;
    private final TCustomSocket socket;

    /**
     * @param address Address in the form of "host:port" to a Impala coordinator. Host may be a hostname or IP.
     * @throws HiveException
     */
    ImpalaConnection(int socketBufferSize, String address, int timeout) throws HiveException {
        String[] addr = address.split(":");

        if (addr.length != 2) {
            throw new HiveException("Address is malformed: '" + address + "' expected format host:port");
        }

        int port;
        try {
            port = Integer.parseInt(addr[1]);
        } catch (Exception e) {
            throw new HiveException("Address is malformed: '" + address + "' expected format host:port, " +
                    "failed to parse port");
        }
        this.socketAddress = InetSocketAddress.createUnresolved(addr[0], port);
        this.socket = new TCustomSocket(socketAddress.getHostString(),
            socketAddress.getPort(), /*timeout*/ 0, /*timeout*/ 0, socketBufferSize);
        this.socket.setSocketTimeout(timeout);
    }

    /**
     * Attempts to open the connection to Impala coordinator. Not idempotent.
     * @throws TException
     */
    public void open() throws TException {
        Preconditions.checkState(!socket.isOpen());
        socket.open();
    }

    /**
     * Closes a previously open connection.
     */
    public void close() {
        socket.close();
    }

    public String toString() {
        return "ImpalaConnection{" + socketAddress + " isOpen: " + socket.isOpen() + "}";
    }

    /**
     * Returns an ImpalaHiveServer2Service.Client using this connection as a socket. Will open the connection if it is
     * not already open.
     * @return ImpalaHiveServer2.Client that is a Thrift interface to the Impala coordinator described by this
     * ImpalaConnection.
     * @throws HiveException
     */
    ImpalaHiveServer2Service.Client getClient() throws HiveException {
        try {
            if (!socket.isOpen()) {
                socket.open();
            }
        } catch (Exception e) {
            throw new HiveException("Failed to connect to impala at address (" + socketAddress +")", e);
        }
        return new ImpalaHiveServer2Service.Client(protocolFactory.getProtocol(socket));
    }
}
