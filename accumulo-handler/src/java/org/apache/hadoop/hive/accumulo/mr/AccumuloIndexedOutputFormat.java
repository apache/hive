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

package org.apache.hadoop.hive.accumulo.mr;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mapred.AccumuloOutputFormat;
import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.accumulo.AccumuloIndexLexicoder;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Extension of AccumuloOutputFormat to support indexing.
 */
public class AccumuloIndexedOutputFormat extends AccumuloOutputFormat {
  private static final Logger LOG =  LoggerFactory.getLogger(AccumuloIndexedOutputFormat.class);
  private static final Class<?> CLASS = AccumuloOutputFormat.class;
  private static final byte[] EMPTY_BYTES = new byte[0];

  public static void setIndexTableName(JobConf job, String tableName) {
    IndexOutputConfigurator.setIndexTableName(CLASS, job, tableName);
  }

  protected static String getIndexTableName(JobConf job) {
    return IndexOutputConfigurator.getIndexTableName(CLASS, job);
  }

  public static void setIndexColumns(JobConf job, String fields) {
    IndexOutputConfigurator.setIndexColumns(CLASS, job, fields);
  }

  protected static String getIndexColumns(JobConf job) {
    return IndexOutputConfigurator.getIndexColumns(CLASS, job);
  }

  public static void setStringEncoding(JobConf job, Boolean isStringEncoding) {
    IndexOutputConfigurator.setRecordEncoding(CLASS, job, isStringEncoding);
  }

  protected static Boolean getStringEncoding(JobConf job) {
    return IndexOutputConfigurator.getRecordEncoding(CLASS, job);
  }

  public RecordWriter<Text, Mutation> getRecordWriter(FileSystem ignored, JobConf job,
                                           String name, Progressable progress) throws IOException {
    try {
      return new AccumuloIndexedOutputFormat.AccumuloRecordWriter(job);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  protected static class AccumuloRecordWriter implements RecordWriter<Text, Mutation> {
    private MultiTableBatchWriter mtbw = null;
    private Map<Text, BatchWriter> bws = null;
    private Text defaultTableName = null;
    private Text indexTableName = null;
    private boolean simulate = false;
    private boolean createTables = false;
    private boolean isStringEncoded = true;
    private long mutCount = 0L;
    private long valCount = 0L;
    private Connector conn;
    private AccumuloIndexDefinition indexDef = null;

    protected AccumuloRecordWriter(JobConf job)
        throws AccumuloException, AccumuloSecurityException, IOException {
      this.isStringEncoded = AccumuloIndexedOutputFormat.getStringEncoding(job).booleanValue();
      this.simulate = AccumuloIndexedOutputFormat.getSimulationMode(job).booleanValue();
      this.createTables = AccumuloIndexedOutputFormat.canCreateTables(job).booleanValue();
      if (this.simulate) {
        LOG.info("Simulating output only. No writes to tables will occur");
      }

      this.bws = new HashMap();
      String tname = AccumuloIndexedOutputFormat.getDefaultTableName(job);
      this.defaultTableName = tname == null ? null : new Text(tname);

      String iname = AccumuloIndexedOutputFormat.getIndexTableName(job);
      if (iname != null) {
        LOG.info("Index Table = {}", iname);
        this.indexTableName = new Text(iname);
        this.indexDef = createIndexDefinition(job, tname, iname);
      }
      if (!this.simulate) {
        this.conn = AccumuloIndexedOutputFormat.getInstance(job)
            .getConnector(AccumuloIndexedOutputFormat.getPrincipal(job),
                          AccumuloIndexedOutputFormat.getAuthenticationToken(job));
        this.mtbw = this.conn.createMultiTableBatchWriter(
            AccumuloIndexedOutputFormat.getBatchWriterOptions(job));
      }
    }

     AccumuloIndexDefinition createIndexDefinition(JobConf job, String tname, String iname) {
      AccumuloIndexDefinition def = new AccumuloIndexDefinition(tname, iname);
      String cols = AccumuloIndexedOutputFormat.getIndexColumns(job);
      LOG.info("Index Cols = {}", cols);
      def.setColumnTuples(cols);
      return def;
    }

    public void write(Text table, Mutation mutation) throws IOException {
      if(table == null || table.toString().isEmpty()) {
        table = this.defaultTableName;
      }

      if(!this.simulate && table == null) {
        throw new IOException("No table or default table specified. Try simulation mode next time");
      } else {
        ++this.mutCount;
        this.valCount += (long)mutation.size();
        this.printMutation(table, mutation);
        if(!this.simulate) {
          if(!this.bws.containsKey(table)) {
            try {
              this.addTable(table);
            } catch (Exception var5) {
              LOG.error("Could not add table", var5);
              throw new IOException(var5);
            }
          }
          if(indexTableName != null && !this.bws.containsKey(indexTableName)) {
            try {
              this.addTable(indexTableName);
            } catch (Exception var6) {
              LOG.error("Could not add index table", var6);
              throw new IOException(var6);
            }
          }

          try {
            ((BatchWriter)this.bws.get(table)).addMutation(mutation);
          } catch (MutationsRejectedException var4) {
            throw new IOException(var4);
          }

          // if this table has an associated index table then attempt to build
          // index mutations
          if (indexTableName != null) {
            List<Mutation> idxMuts = getIndexMutations(mutation);
            if (!idxMuts.isEmpty()) {
              try {
                BatchWriter writer = this.bws.get(indexTableName);
                for (Mutation m : idxMuts) {
                  writer.addMutation(m);
                }
              } catch (MutationsRejectedException var4) {
                throw new IOException(var4);
              }
            }
          }
        }
      }
    }

    public void addTable(Text tableName) throws AccumuloException, AccumuloSecurityException {
      if(this.simulate) {
        LOG.info("Simulating adding table: {}", tableName);
      } else {
        LOG.debug("Adding table: {}", tableName);
        BatchWriter bw = null;
        String table = tableName.toString();
        if(this.createTables && !this.conn.tableOperations().exists(table)) {
          try {
            this.conn.tableOperations().create(table);
          } catch (AccumuloSecurityException var8) {
            LOG.error("Accumulo security violation creating {}", table, var8);
            throw var8;
          } catch (TableExistsException var9) {
            LOG.warn("Table Exists {}", table, var9);
          }
        }

        try {
          bw = this.mtbw.getBatchWriter(table);
        } catch (TableNotFoundException var5) {
          LOG.error("Accumulo table {} doesn't exist and cannot be created.", table, var5);
          throw new AccumuloException(var5);
        }

        if(bw != null) {
          this.bws.put(tableName, bw);
        }

      }
    }

    private int printMutation(Text table, Mutation m) {
      if(LOG.isTraceEnabled()) {
        LOG.trace("Table {} row key: {}", table, this.hexDump(m.getRow()));
        Iterator itr = m.getUpdates().iterator();

        while(itr.hasNext()) {
          ColumnUpdate cu = (ColumnUpdate)itr.next();
          LOG.trace("Table {} column: {}:{}",
              table, this.hexDump(cu.getColumnFamily()), this.hexDump(cu.getColumnQualifier()));
          LOG.trace("Table {} security: {}", table, new ColumnVisibility(cu.getColumnVisibility()).toString());
          LOG.trace("Table {} value: {}", table, this.hexDump(cu.getValue()));
        }
      }

      return m.getUpdates().size();
    }

    private List<Mutation> getIndexMutations(Mutation baseMut) {
      List indexMuts = new ArrayList<Mutation>();

      // nothing to do if there is not a index definition for this table
      if (null != indexDef) {

        byte[] rowId = baseMut.getRow();


        for (ColumnUpdate cu : baseMut.getUpdates()) {
          String cf = new String(cu.getColumnFamily());
          String cq = new String(cu.getColumnQualifier());

          // if this columnFamily/columnQualifier pair is defined in the index build a new mutation
          // so key=value, cf=columnFamily_columnQualifer, cq=rowKey, cv=columnVisibility value=[]
          String colType = indexDef.getColType(cf, cq);
          if (colType != null) {
            LOG.trace("Building index for column {}:{}", cf, cq);
            Mutation m = new Mutation(AccumuloIndexLexicoder.encodeValue(cu.getValue(), colType,
                                               isStringEncoded));
            String colFam = cf + "_" + cq;
            m.put(colFam.getBytes(), rowId, new ColumnVisibility(cu.getColumnVisibility()),
                  EMPTY_BYTES);
            indexMuts.add(m);
          }
        }
      }
      return indexMuts;
    }

    private String hexDump(byte[] ba) {
      StringBuilder sb = new StringBuilder();
      byte[] arr = ba;
      int len = ba.length;

      for(int i = 0; i < len; ++i) {
        byte b = arr[i];
        if(b > 32 && b < 126) {
          sb.append((char)b);
        } else {
          sb.append(String.format("x%02x", new Object[]{Byte.valueOf(b)}));
        }
      }

      return sb.toString();
    }

    public void close(Reporter reporter) throws IOException {
      LOG.debug("mutations written: {}, values written: {}", this.mutCount, this.valCount);
      if(!this.simulate) {
        try {
          this.mtbw.close();
        } catch (MutationsRejectedException var7) {
          if(var7.getAuthorizationFailuresMap().size() >= 0) {
            Map tables = new HashMap();

            Map.Entry ke;
            Object secCodes;
            for(Iterator itr = var7.getAuthorizationFailuresMap().entrySet().iterator();
                itr.hasNext(); ((Set)secCodes).addAll((Collection)ke.getValue())) {
              ke = (Map.Entry)itr.next();
              secCodes = (Set)tables.get(((KeyExtent)ke.getKey()).getTableId().toString());
              if(secCodes == null) {
                secCodes = new HashSet();
                tables.put(((KeyExtent)ke.getKey()).getTableId().toString(), secCodes);
              }
            }

            LOG.error("Not authorized to write to tables {}", tables);
          }

          if(var7.getConstraintViolationSummaries().size() > 0) {
            LOG.error("Constraint violations : {}", var7.getConstraintViolationSummaries().size());
          }
          throw new IOException(var7);
        }
      }
    }
  }
}
