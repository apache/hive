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
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
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

/**
 * This class is nothing but a delegate for the enclosed proxy,
 * which is created upon setting the configuration.
 */
public class RevisionManagerEndpointClient implements RevisionManager, Configurable {

  private final RPCConverter rpcConverter = new RPCConverter();
  private Configuration conf;
  private HTable htable;

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public void initialize(Configuration conf) {
    // do nothing
  }

  @Override
  public void open() throws IOException {
    // clone to adjust RPC settings unique to proxy
    Configuration clonedConf = new Configuration(conf);
    clonedConf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 1); // do not retry RPC
    htable = new HTable(clonedConf, TableName.META_TABLE_NAME.getNameAsString());
  }

  @Override
  public void close() throws IOException {
    htable.close();
  }

  @Override
  public void createTable(final String table, final List<String> columnFamilies) throws IOException {
    call(new Batch.Call<RevisionManagerEndpointService, Void>() {
      @Override
      public Void call(RevisionManagerEndpointService service)
        throws IOException {
        ServerRpcController controller = new ServerRpcController();
        BlockingRpcCallback<CreateTableResponse> done =
            new BlockingRpcCallback<CreateTableResponse>();
        CreateTableRequest request = CreateTableRequest.newBuilder()
                .setTableName(table).addAllColumnFamilies(columnFamilies).build();
        service.createTable(controller, request, done);
        blockOnResponse(done, controller);
        return null;
      }
    });
  }

  @Override
  public void dropTable(final String table) throws IOException {
    call(new Batch.Call<RevisionManagerEndpointService, Void>() {
      @Override
      public Void call(RevisionManagerEndpointService service)
        throws IOException {
        ServerRpcController controller = new ServerRpcController();
        BlockingRpcCallback<DropTableResponse> done =
            new BlockingRpcCallback<DropTableResponse>();
        DropTableRequest request = DropTableRequest.newBuilder()
              .setTableName(table).build();
        service.dropTable(null, request, done);
        blockOnResponse(done, controller);
        return null;
      }
    });
  }

  @Override
  public Transaction beginWriteTransaction(final String table, final List<String> families) throws IOException {
    return beginWriteTransaction(table, families, null);
  }

  @Override
  public Transaction beginWriteTransaction(final String table, final List<String> families, final Long keepAlive)
    throws IOException {
    return call(new Batch.Call<RevisionManagerEndpointService, Transaction>() {
      @Override
      public Transaction call(RevisionManagerEndpointService service)
        throws IOException {
        ServerRpcController controller = new ServerRpcController();
        BlockingRpcCallback<BeginWriteTransactionResponse> done =
            new BlockingRpcCallback<BeginWriteTransactionResponse>();
        BeginWriteTransactionRequest.Builder builder = BeginWriteTransactionRequest.newBuilder()
              .setTableName(table)
              .addAllColumnFamilies(families);
        if(keepAlive != null) {
          builder.setKeepAlive(keepAlive);
        }
        service.beginWriteTransaction(controller, builder.build(), done);
        return rpcConverter.convertTransaction(blockOnResponse(done, controller).getTransaction());
      }
    });
  }

  @Override
  public void commitWriteTransaction(final Transaction transaction) throws IOException {
    call(new Batch.Call<RevisionManagerEndpointService, Void>() {
      @Override
      public Void call(RevisionManagerEndpointService service)
        throws IOException {
        ServerRpcController controller = new ServerRpcController();
        BlockingRpcCallback<CommitWriteTransactionResponse> done =
            new BlockingRpcCallback<CommitWriteTransactionResponse>();
        CommitWriteTransactionRequest request = CommitWriteTransactionRequest.newBuilder()
              .setTransaction(rpcConverter.convertTransaction(transaction)).build();
        service.commitWriteTransaction(controller, request, done);
        blockOnResponse(done, controller);
        return null;
      }
    });
  }

  @Override
  public void abortWriteTransaction(final Transaction transaction) throws IOException {
    call(new Batch.Call<RevisionManagerEndpointService, Void>() {
      @Override
      public Void call(RevisionManagerEndpointService service)
        throws IOException {
        ServerRpcController controller = new ServerRpcController();
        BlockingRpcCallback<AbortWriteTransactionResponse> done =
            new BlockingRpcCallback<AbortWriteTransactionResponse>();
        AbortWriteTransactionRequest request = AbortWriteTransactionRequest.newBuilder()
              .setTransaction(rpcConverter.convertTransaction(transaction)).build();
        service.abortWriteTransaction(controller, request, done);
        blockOnResponse(done, controller);
        return null;
      }
    });
  }

  @Override
  public List<FamilyRevision> getAbortedWriteTransactions(final String table, final String columnFamily)
    throws IOException {
    return call(new Batch.Call<RevisionManagerEndpointService, List<FamilyRevision>>() {
      @Override
      public List<FamilyRevision> call(RevisionManagerEndpointService service)
        throws IOException {
        ServerRpcController controller = new ServerRpcController();
        BlockingRpcCallback<GetAbortedWriteTransactionsResponse> done =
            new BlockingRpcCallback<GetAbortedWriteTransactionsResponse>();
        GetAbortedWriteTransactionsRequest request = GetAbortedWriteTransactionsRequest.newBuilder()
              .setTableName(table)
              .setColumnFamily(columnFamily)
              .build();
        service.getAbortedWriteTransactions(controller, request, done);
        return rpcConverter.convertFamilyRevisions(blockOnResponse(done, controller).getFamilyRevisionsList());
      }
    });
  }

  @Override
  public TableSnapshot createSnapshot(String tableName) throws IOException {
    return createSnapshot(tableName, null);
  }

  @Override
  public TableSnapshot createSnapshot(final String tableName, final Long revision) throws IOException {
    return call(new Batch.Call<RevisionManagerEndpointService, TableSnapshot>() {
      @Override
      public TableSnapshot call(RevisionManagerEndpointService service)
        throws IOException {
        ServerRpcController controller = new ServerRpcController();
        BlockingRpcCallback<CreateSnapshotResponse> done =
            new BlockingRpcCallback<CreateSnapshotResponse>();
        CreateSnapshotRequest.Builder builder = CreateSnapshotRequest.newBuilder()
              .setTableName(tableName);
        if(revision != null) {
          builder.setRevision(revision);
        }
        service.createSnapshot(controller, builder.build(), done);
        return rpcConverter.convertTableSnapshot(blockOnResponse(done, controller).getTableSnapshot());
      }
    });
  }

  @Override
  public void keepAlive(final Transaction transaction) throws IOException {
    call(new Batch.Call<RevisionManagerEndpointService, Void>() {
      @Override
      public Void call(RevisionManagerEndpointService service)
        throws IOException {
        ServerRpcController controller = new ServerRpcController();
        BlockingRpcCallback<KeepAliveTransactionResponse> done =
            new BlockingRpcCallback<KeepAliveTransactionResponse>();
        KeepAliveTransactionRequest request = KeepAliveTransactionRequest.newBuilder()
              .setTransaction(rpcConverter.convertTransaction(transaction)).build();
        service.keepAliveTransaction(controller, request, done);
        blockOnResponse(done, controller);
        return null;
      }
    });
  }
  private <R> R blockOnResponse(BlockingRpcCallback<R> done, ServerRpcController controller)
    throws IOException {
    R response = done.get();
    if(controller.failedOnException()) {
      throw controller.getFailedOn();
    }
    if(controller.failed()) {
      String error = controller.errorText();
      if(error == null) {
        error = "Server indicated failure but error text was empty";
      }
      throw new RuntimeException(error);
    }
    return response;
  }
  private <R> R call(Batch.Call<RevisionManagerEndpointService, R> callable) throws IOException {
    try {
      Map<byte[], R> result = htable.coprocessorService(RevisionManagerEndpointService.class, null, null, callable);
      if(result.isEmpty()) {
        return null;
      }
      return result.values().iterator().next();
    } catch(IOException e) {
      throw (IOException)e;
    } catch(RuntimeException e) {
      throw (RuntimeException)e;
    } catch(Error e) {
      throw (Error)e;
    } catch(Throwable throwable) {
      throw new RuntimeException(throwable);
    }
  }

}
