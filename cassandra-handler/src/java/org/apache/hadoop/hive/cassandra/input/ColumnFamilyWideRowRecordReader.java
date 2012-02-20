package org.apache.hadoop.hive.cassandra.input;

/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */


import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.cassandra.auth.IAuthenticator;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.hadoop.ColumnFamilyRecordReader;
import org.apache.cassandra.hadoop.ColumnFamilySplit;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.thrift.AuthenticationRequest;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.CounterColumn;
import org.apache.cassandra.thrift.CounterSuperColumn;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SuperColumn;
import org.apache.cassandra.thrift.TBinaryProtocol;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.mortbay.log.Log;

import com.google.common.collect.AbstractIterator;

public class ColumnFamilyWideRowRecordReader extends ColumnFamilyRecordReader {
  public static final int CASSANDRA_HADOOP_MAX_KEY_SIZE_DEFAULT = 8192;

  private ColumnFamilySplit split;
  private WideRowIterator iter;
  private Pair<ByteBuffer, SortedMap<ByteBuffer, IColumn>> currentRow;
  private SlicePredicate predicate;
  private int totalRowCount; // total number of rows to fetch
  private int batchRowCount; // fetch this many per batch
  private int rowPageSize; // total number of columns to read
  private ByteBuffer startSlicePredicate;
  private String cfName;
  private String keyspace;
  private TSocket socket;
  private Cassandra.Client client;
  private ConsistencyLevel consistencyLevel;
  private int keyBufferSize = 8192;

  public ColumnFamilyWideRowRecordReader() {
    this(ColumnFamilyWideRowRecordReader.CASSANDRA_HADOOP_MAX_KEY_SIZE_DEFAULT);
  }

  public ColumnFamilyWideRowRecordReader(int keyBufferSize) {
    super();
    this.keyBufferSize = keyBufferSize;
  }

  @Override
  public void close() {
    if (socket != null && socket.isOpen()) {
      socket.close();
      socket = null;
      client = null;
    }
  }

  @Override
  public ByteBuffer getCurrentKey() {
    return currentRow.left;
  }

  @Override
  public SortedMap<ByteBuffer, IColumn> getCurrentValue() {
    return currentRow.right;
  }

  @Override
  public float getProgress() {
    // the progress is likely to be reported slightly off the actual but close enough
    return iter.rowsRead() > totalRowCount ? 1.0f : ((float) iter.rowsRead()) / totalRowCount;
  }

  static boolean isSliceRangePredicate(SlicePredicate predicate) {
    if (predicate == null) {
      return false;
    }

    if (predicate.isSetColumn_names() && predicate.getSlice_range() == null) {
      return false;
    }

    if (predicate.getSlice_range() == null) {
      return false;
    }

    byte[] start = predicate.getSlice_range().getStart();
    byte[] finish = predicate.getSlice_range().getFinish();
    if (start != null && finish != null) {
      return true;
    }

    return false;
  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
    this.split = (ColumnFamilySplit) split;
    Configuration conf = context.getConfiguration();
    predicate = ConfigHelper.getInputSlicePredicate(conf);
    if (!isSliceRangePredicate(predicate)) {
      throw new AssertionError("WideRowsRequire a slice range");
    }


    totalRowCount = ConfigHelper.getInputSplitSize(conf);
    Log.info("total rows = "+totalRowCount);
    batchRowCount = 1;
    rowPageSize = predicate.getSlice_range().getCount();
    startSlicePredicate = predicate.getSlice_range().start;
    cfName = ConfigHelper.getInputColumnFamily(conf);
    consistencyLevel = ConsistencyLevel.valueOf(ConfigHelper.getReadConsistencyLevel(conf));


    keyspace = ConfigHelper.getInputKeyspace(conf);

    try {
      // only need to connect once
      if (socket != null && socket.isOpen()) {
        return;
      }

      // create connection using thrift
      String location = getLocation();
      socket = new TSocket(location, ConfigHelper.getRpcPort(conf));
      TBinaryProtocol binaryProtocol = new TBinaryProtocol(new TFramedTransport(socket));
      client = new Cassandra.Client(binaryProtocol);
      socket.open();

      // log in
      client.set_keyspace(keyspace);
      if (ConfigHelper.getInputKeyspaceUserName(conf) != null) {
        Map<String, String> creds = new HashMap<String, String>();
        creds.put(IAuthenticator.USERNAME_KEY, ConfigHelper.getInputKeyspaceUserName(conf));
        creds.put(IAuthenticator.PASSWORD_KEY, ConfigHelper.getInputKeyspacePassword(conf));
        AuthenticationRequest authRequest = new AuthenticationRequest(creds);
        client.login(authRequest);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    iter = new WideRowIterator();
  }

  @Override
  public boolean nextKeyValue() throws IOException {
    if (!iter.hasNext()) {
      return false;
    }
    currentRow = iter.next();
    return true;
  }

  // we don't use endpointsnitch since we are trying to support hadoop nodes that are
  // not necessarily on Cassandra machines, too. This should be adequate for single-DC clusters, at
  // least.
  private String getLocation() {
    ArrayList<InetAddress> localAddresses = new ArrayList<InetAddress>();
    try {
      Enumeration<NetworkInterface> nets = NetworkInterface.getNetworkInterfaces();
      while (nets.hasMoreElements()) {
        localAddresses.addAll(Collections.list(nets.nextElement().getInetAddresses()));
      }
    } catch (SocketException e) {
      throw new AssertionError(e);
    }

    for (InetAddress address : localAddresses) {
      for (String location : split.getLocations()) {
        InetAddress locationAddress = null;
        try {
          locationAddress = InetAddress.getByName(location);
        } catch (UnknownHostException e) {
          throw new AssertionError(e);
        }
        if (address.equals(locationAddress)) {
          return location;
        }
      }
    }
    return split.getLocations()[0];
  }

  private class WideRowIterator extends
      AbstractIterator<Pair<ByteBuffer, SortedMap<ByteBuffer, IColumn>>> {
    private List<KeySlice> rows;
    private String startToken;
    private final ByteBuffer nextStartSlice = null;
    private int columnsRead = 0;
    private int totalRead = 0;
    private final AbstractType comparator;
    private final AbstractType subComparator;
    private final IPartitioner partitioner;

    private WideRowIterator() {
      try {
        partitioner = FBUtilities.newPartitioner(client.describe_partitioner());

        CfDef cf_def = findCfDef(ks_def, cfName);
        if (cf_def == null) {
          throw new RuntimeException("ColumnFamily named " + cfName + " wasn't found in keyspace " + ks_def.name);
        }

        comparator = TypeParser.parse(cf_def.comparator_type);
        subComparator = cf_def.subcomparator_type == null ? null : TypeParser
            .parse(cf_def.subcomparator_type);
      } catch (ConfigurationException e) {
        throw new RuntimeException("unable to load sub/comparator", e);
      } catch (TException e) {
        throw new RuntimeException("error communicating via Thrift", e);
      } catch (Exception e) {
        throw new RuntimeException("unable to load keyspace " + keyspace, e);
      }
    }

    private static CfDef findCfDef(KsDef ks_def, String cfName)
    {
        for (CfDef cfDef : ks_def.cf_defs)
        {
            if (cfDef.name.equals(cfName)) {
              return cfDef;
            }
        }

       return null;
    }

    private void maybeInit() {
      // check if we need another row
      if (rows != null && columnsRead < rowPageSize) {
        columnsRead = 0;
        startToken = partitioner.getTokenFactory().toString(partitioner.getToken(rows.get(0).key));
        predicate.getSlice_range().setStart(startSlicePredicate);
        rows = null;
        totalRead++;
      }

      if (startToken == null) {
        startToken = split.getStartToken();
      } else if (startToken.equals(split.getEndToken()) && rows == null) {
        // reached end of the split
        return;
      }

      KeyRange keyRange = new KeyRange(batchRowCount)
                                .setStart_token(startToken)
                                .setEnd_token(split.getEndToken());
      try {
        rows = client.get_range_slices(new ColumnParent(cfName),
                                               predicate,
                                               keyRange,
                                               consistencyLevel);

        // nothing new? reached the end
        if (rows.isEmpty()) {
          rows = null;
          return;
        }

        // prepare for the next slice to be read
        KeySlice row = rows.get(0);

        if (row.getColumnsSize() > 1) {

          ColumnOrSuperColumn cosc = row.getColumns().get(row.getColumnsSize() - 1);

          //prepare next slice
          if (cosc.column != null) {
            predicate.slice_range.start = cosc.column.name;
          }

          if (cosc.super_column != null) {
            predicate.slice_range.start = cosc.super_column.name;
          }

          if (cosc.counter_column != null) {
            predicate.slice_range.start = cosc.counter_column.name;
          }

          if (cosc.counter_super_column != null) {
            predicate.slice_range.start = cosc.counter_super_column.name;
          }

          columnsRead = row.getColumnsSize();

          //If we've hit the max columns then rm the last column
          //to make sure we don't know where to start next without overlap
          if (columnsRead == rowPageSize) {
            row.getColumns().remove(row.getColumnsSize() - 1);
          }
        }

      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    /**
     * @return total number of rows read by this record reader
     */
    public int rowsRead() {
      return totalRead;
    }

    @Override
    protected Pair<ByteBuffer, SortedMap<ByteBuffer, IColumn>> computeNext() {
      maybeInit();
      if (rows == null) {
        return endOfData();
      }

      KeySlice ks = rows.get(0);
      SortedMap<ByteBuffer, IColumn> map = new TreeMap<ByteBuffer, IColumn>(comparator);
      for (ColumnOrSuperColumn cosc : ks.columns) {
        IColumn column = unthriftify(cosc);
        map.put(column.name(), column);
      }
      return new Pair<ByteBuffer, SortedMap<ByteBuffer, IColumn>>(ks.key, map);
    }

    private IColumn unthriftify(ColumnOrSuperColumn cosc) {
      if (cosc.counter_column != null) {
        return unthriftifyCounter(cosc.counter_column);
      }
      if (cosc.counter_super_column != null) {
        return unthriftifySuperCounter(cosc.counter_super_column);
      }
      if (cosc.super_column != null) {
        return unthriftifySuper(cosc.super_column);
      }
      assert cosc.column != null;
      return unthriftifySimple(cosc.column);
    }

    private IColumn unthriftifySuper(SuperColumn super_column) {
      org.apache.cassandra.db.SuperColumn sc = new org.apache.cassandra.db.SuperColumn(
          super_column.name, subComparator);
      for (Column column : super_column.columns) {
        sc.addColumn(unthriftifySimple(column));
      }
      return sc;
    }

    private IColumn unthriftifySimple(Column column) {
      return new org.apache.cassandra.db.Column(column.name, column.value, column.timestamp);
    }

    private IColumn unthriftifyCounter(CounterColumn column) {
      // CounterColumns read the nodeID from the System table, so need the StorageService running
      // and access
      // to cassandra.yaml. To avoid a Hadoop needing access to yaml return a regular Column.
      return new org.apache.cassandra.db.Column(column.name, ByteBufferUtil.bytes(column.value), 0);
    }

    private IColumn unthriftifySuperCounter(CounterSuperColumn superColumn) {
      org.apache.cassandra.db.SuperColumn sc = new org.apache.cassandra.db.SuperColumn(
          superColumn.name, subComparator);
      for (CounterColumn column : superColumn.columns) {
        sc.addColumn(unthriftifyCounter(column));
      }
      return sc;
    }
  }


  // Because the old Hadoop API wants us to write to the key and value
  // and the new asks for them, we need to copy the output of the new API
  // to the old. Thus, expect a small performance hit.
  // And obviously this wouldn't work for wide rows. But since ColumnFamilyInputFormat
  // and ColumnFamilyRecordReader don't support them, it should be fine for now.
  @Override
  public boolean next(ByteBuffer key, SortedMap<ByteBuffer, IColumn> value) throws IOException {
    if (this.nextKeyValue()) {
      key.clear();
      key.put(this.getCurrentKey());
      key.rewind();

      value.clear();
      value.putAll(this.getCurrentValue());

      return true;
    }
    return false;
  }

  @Override
  public ByteBuffer createKey() {
    return ByteBuffer.wrap(new byte[this.keyBufferSize]);
  }

  @Override
  public SortedMap<ByteBuffer, IColumn> createValue() {
    return new TreeMap<ByteBuffer, IColumn>();
  }

  @Override
  public long getPos() throws IOException {
    return (long) iter.rowsRead();
  }
}
