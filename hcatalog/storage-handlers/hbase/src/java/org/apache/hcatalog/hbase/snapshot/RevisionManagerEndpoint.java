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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hcatalog.hbase.snapshot;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hcatalog.hbase.snapshot.RevisionManagerEndpointProtos.AbortWriteTransactionRequest;
import org.apache.hcatalog.hbase.snapshot.RevisionManagerEndpointProtos.AbortWriteTransactionResponse;
import org.apache.hcatalog.hbase.snapshot.RevisionManagerEndpointProtos.BeginWriteTransactionRequest;
import org.apache.hcatalog.hbase.snapshot.RevisionManagerEndpointProtos.BeginWriteTransactionResponse;
import org.apache.hcatalog.hbase.snapshot.RevisionManagerEndpointProtos.CommitWriteTransactionRequest;
import org.apache.hcatalog.hbase.snapshot.RevisionManagerEndpointProtos.CommitWriteTransactionResponse;
import org.apache.hcatalog.hbase.snapshot.RevisionManagerEndpointProtos.CreateSnapshotRequest;
import org.apache.hcatalog.hbase.snapshot.RevisionManagerEndpointProtos.CreateSnapshotResponse;
import org.apache.hcatalog.hbase.snapshot.RevisionManagerEndpointProtos.CreateTableRequest;
import org.apache.hcatalog.hbase.snapshot.RevisionManagerEndpointProtos.CreateTableResponse;
import org.apache.hcatalog.hbase.snapshot.RevisionManagerEndpointProtos.DropTableRequest;
import org.apache.hcatalog.hbase.snapshot.RevisionManagerEndpointProtos.DropTableResponse;
import org.apache.hcatalog.hbase.snapshot.RevisionManagerEndpointProtos.GetAbortedWriteTransactionsRequest;
import org.apache.hcatalog.hbase.snapshot.RevisionManagerEndpointProtos.GetAbortedWriteTransactionsResponse;
import org.apache.hcatalog.hbase.snapshot.RevisionManagerEndpointProtos.KeepAliveTransactionRequest;
import org.apache.hcatalog.hbase.snapshot.RevisionManagerEndpointProtos.KeepAliveTransactionResponse;
import org.apache.hcatalog.hbase.snapshot.RevisionManagerEndpointProtos.RevisionManagerEndpointService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;

/**
 * Implementation of RevisionManager as HBase RPC endpoint. This class will control the lifecycle of
 * and delegate to the actual RevisionManager implementation and make it available as a service
 * hosted in the HBase region server (instead of running it in the client (storage handler).
 * In the case of {@link ZKBasedRevisionManager} now only the region servers need write access to
 * manage revision data.
 */
public class RevisionManagerEndpoint extends RevisionManagerEndpointService implements Coprocessor, CoprocessorService {

  private static final Logger LOGGER =
    LoggerFactory.getLogger(RevisionManagerEndpoint.class.getName());

  private final RPCConverter rpcConverter = new RPCConverter();
  private RevisionManager rmImpl = null;

  @Override
  public void start(CoprocessorEnvironment env) {
    try {
      Configuration conf = RevisionManagerConfiguration.create(env.getConfiguration());
      String className = conf.get(RMConstants.REVISION_MGR_ENDPOINT_IMPL_CLASS,
        ZKBasedRevisionManager.class.getName());
      LOGGER.info("Using Revision Manager implementation: {}", className);
      rmImpl = RevisionManagerFactory.getOpenedRevisionManager(className, conf);
    } catch (IOException e) {
      LOGGER.error("Failed to initialize revision manager", e);
    }
  }

  @Override
  public void stop(CoprocessorEnvironment env) {
    try {
      if (rmImpl != null) {
        rmImpl.close();
      }
    } catch (IOException e) {
      LOGGER.warn("Error closing revision manager.", e);
    }
  }

  @Override
  public Service getService() {
    return this;
  }

  @Override
  public void createTable(RpcController controller,
      CreateTableRequest request, RpcCallback<CreateTableResponse> done) {
    if(rmImpl != null) {
      try {
        rmImpl.createTable(request.getTableName(), request.getColumnFamiliesList());
        done.run(CreateTableResponse.newBuilder().build());
      } catch(IOException e) {
        ResponseConverter.setControllerException(controller, e);
      }
    }
  }

  @Override
  public void dropTable(RpcController controller, DropTableRequest request,
      RpcCallback<DropTableResponse> done) {
    if(rmImpl != null) {
      try {
        rmImpl.dropTable(request.getTableName());
        done.run(DropTableResponse.newBuilder().build());
      } catch(IOException e) {
        ResponseConverter.setControllerException(controller, e);
      }
    }
  }

  @Override
  public void beginWriteTransaction(RpcController controller,
      BeginWriteTransactionRequest request,
      RpcCallback<BeginWriteTransactionResponse> done) {
    if(rmImpl != null) {
      try {
        Transaction transaction;
        if(request.hasKeepAlive()) {
          transaction = rmImpl.beginWriteTransaction(request.getTableName(), request.getColumnFamiliesList(),
              request.getKeepAlive());
        } else {
          transaction = rmImpl.beginWriteTransaction(request.getTableName(), request.getColumnFamiliesList());
        }
        done.run(BeginWriteTransactionResponse.newBuilder()
                .setTransaction(rpcConverter.convertTransaction(transaction)).build());
      } catch(IOException e) {
        ResponseConverter.setControllerException(controller, e);
      }
    }
  }

  @Override
  public void commitWriteTransaction(RpcController controller,
      CommitWriteTransactionRequest request,
      RpcCallback<CommitWriteTransactionResponse> done) {
    if(rmImpl != null) {
      try {
        rmImpl.commitWriteTransaction(rpcConverter.convertTransaction(request.getTransaction()));
        done.run(CommitWriteTransactionResponse.newBuilder().build());
      } catch(IOException e) {
        ResponseConverter.setControllerException(controller, e);
      }
    }
  }

  @Override
  public void abortWriteTransaction(RpcController controller,
      AbortWriteTransactionRequest request,
      RpcCallback<AbortWriteTransactionResponse> done) {
    if(rmImpl != null) {
      try {
        rmImpl.abortWriteTransaction(rpcConverter.convertTransaction(request.getTransaction()));
        done.run(AbortWriteTransactionResponse.newBuilder().build());
      } catch(IOException e) {
        ResponseConverter.setControllerException(controller, e);
      }
    }
  }

  @Override
  public void getAbortedWriteTransactions(RpcController controller,
      GetAbortedWriteTransactionsRequest request,
      RpcCallback<GetAbortedWriteTransactionsResponse> done) {
    if(rmImpl != null) {
      try {
        rmImpl.getAbortedWriteTransactions(request.getTableName(), request.getColumnFamily());
        done.run(GetAbortedWriteTransactionsResponse.newBuilder().build());
      } catch(IOException e) {
        ResponseConverter.setControllerException(controller, e);
      }
    }
  }

  @Override
  public void createSnapshot(RpcController controller,
      CreateSnapshotRequest request, RpcCallback<CreateSnapshotResponse> done) {
    if(rmImpl != null) {
      try {
        TableSnapshot snapshot;
        if(request.hasRevision()) {
          snapshot = rmImpl.createSnapshot(request.getTableName(), request.getRevision());
        } else {
          snapshot = rmImpl.createSnapshot(request.getTableName());
        }
        done.run(CreateSnapshotResponse.newBuilder()
                .setTableSnapshot(rpcConverter.convertTableSnapshot(snapshot)).build());
      } catch(IOException e) {
        ResponseConverter.setControllerException(controller, e);
      }
    }
  }

  @Override
  public void keepAliveTransaction(RpcController controller,
      KeepAliveTransactionRequest request,
      RpcCallback<KeepAliveTransactionResponse> done) {
    if(rmImpl != null) {
      try {
        rmImpl.keepAlive(rpcConverter.convertTransaction(request.getTransaction()));
        done.run(KeepAliveTransactionResponse.newBuilder().build());
      } catch(IOException e) {
        ResponseConverter.setControllerException(controller, e);
      }
    }
  }

}
