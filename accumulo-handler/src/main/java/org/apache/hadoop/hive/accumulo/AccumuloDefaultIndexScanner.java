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

package org.apache.hadoop.hive.accumulo;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.accumulo.serde.AccumuloIndexParameters;
import org.apache.hadoop.hive.accumulo.serde.AccumuloSerDeParameters;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.EMPTY_SET;

/**
 * This default index scanner expects indexes to be in the same format as presto's
 * accumulo index tables defined as:
 * [rowid=field value] [cf=cfname_cqname] [cq=rowid] [visibility] [value=""]
 * <p>
 * This handler looks for the following hive serde properties:
 * 'accumulo.indextable.name' = 'table_idx' (required - name of the corresponding index table)
 * 'accumulo.indexed.columns' = 'name,age,phone' (optional - comma separated list of indexed
 *                      hive columns if not defined or defined as '*' all columns are
 *                      assumed to be indexed )
 * 'accumulo.index.rows.max' = '20000' (optional - maximum number of match indexes to use
 *                      before converting to a full table scan default=20000'
 *                      Note: This setting controls the size of the in-memory list of rowids
 *                      each search predicate. Using large values for this setting or having
 *                      very large rowid values may require additional memory to prevent
 *                      out of memory errors
 * 'accumulo.index.scanner'  = 'org.apache.hadoop.hive.accumulo.AccumuloDefaultIndexScanner'
 *                      (optional - name of the index scanner)
 * <p>
 * To implement your own index table scheme it should be as simple as sub-classing
 * this class and overriding getIndexRowRanges() and optionally init() if you need more
 * config settings
 */
public class AccumuloDefaultIndexScanner implements AccumuloIndexScanner {
  private static final Logger LOG = LoggerFactory.getLogger(AccumuloDefaultIndexScanner.class);

  private AccumuloConnectionParameters connectParams;
  private AccumuloIndexParameters indexParams;
  private int maxRowIds;
  private Authorizations auths;
  private String indexTable;
  private Set<String> indexColumns = EMPTY_SET;
  private Connector connect;
  private Map<String, String> colMap;

  /**
   * Initialize object based on configuration.
   *
   * @param conf - Hive configuration
   */
  @Override
  public void init(Configuration conf) {
    connectParams = new AccumuloConnectionParameters(conf);
    indexParams = new AccumuloIndexParameters(conf);
    maxRowIds = indexParams.getMaxIndexRows();
    auths = indexParams.getTableAuths();
    indexTable = indexParams.getIndexTable();
    indexColumns = indexParams.getIndexColumns();
    colMap = createColumnMap(conf);

  }

  /**
   * Get a list of rowid ranges by scanning a column index.
   *
   * @param column     - the hive column name
   * @param indexRange - Key range to scan on the index table
   * @return List of matching rowid ranges or null if too many matches found
   * if index values are not found a newline range is added to list to
   * short-circuit the query
   */
  @Override
  public List<Range> getIndexRowRanges(String column, Range indexRange) {
    List<Range> rowIds = new ArrayList<Range>();
    Scanner scan = null;
    String col = this.colMap.get(column);

    if (col != null) {

      try {
        LOG.debug("Searching tab=" + indexTable + " column=" + column + " range=" + indexRange);
        Connector conn = getConnector();
        scan = conn.createScanner(indexTable, auths);
        scan.setRange(indexRange);
        Text cf = new Text(col);
        LOG.debug("Using Column Family=" + toString());
        scan.fetchColumnFamily(cf);

        for (Map.Entry<Key, Value> entry : scan) {

          rowIds.add(new Range(entry.getKey().getColumnQualifier()));

          // if we have too many results return null for a full scan
          if (rowIds.size() > maxRowIds) {
            return null;
          }
        }

        // no hits on the index so return a no match range
        if (rowIds.isEmpty()) {
          LOG.debug("Found 0 index matches");
        } else {
          LOG.debug("Found " + rowIds.size() + " index matches");
        }

        return rowIds;
      } catch (AccumuloException | AccumuloSecurityException | TableNotFoundException e) {
        LOG.error("Failed to scan index table: " + indexTable, e);
      } finally {
        if (scan != null) {
          scan.close();
        }
      }
    }

    // assume the index is bad and do a full scan
    LOG.debug("Index lookup failed for table " + indexTable);
    return null;
  }

  /**
   * Test if column is defined in the index table.
   *
   * @param column - hive column name
   * @return true if the column is defined as part of the index table
   */
  @Override
  public boolean isIndexed(String column) {
    return indexTable != null
        && (indexColumns.isEmpty() || indexColumns.contains("*")
        || this.indexColumns.contains(column.toLowerCase())
        || this.indexColumns.contains(column.toUpperCase()));

  }

  protected Map<String, String> createColumnMap(Configuration conf) {
    Map<String, String> colsMap = new HashMap<String, String>();
    String accColString = conf.get(AccumuloSerDeParameters.COLUMN_MAPPINGS);
    if (accColString != null && !accColString.trim().isEmpty()) {
      String[] accCols = accColString.split(",");
      String[] hiveCols = conf.get(serdeConstants.LIST_COLUMNS).split(",");
      for (int i = 0; i < accCols.length; i++) {
        colsMap.put(hiveCols[i], accCols[i].replace(':', '_'));
      }
    }
    return colsMap;
  }

  protected Connector getConnector() throws AccumuloSecurityException, AccumuloException {
    if (connect == null) {
      connect = connectParams.getConnector();
    }
    return connect;
  }

  public void setConnectParams(AccumuloConnectionParameters connectParams) {
    this.connectParams = connectParams;
  }

  public AccumuloConnectionParameters getConnectParams() {
    return connectParams;
  }

  public AccumuloIndexParameters getIndexParams() {
    return indexParams;
  }

  public int getMaxRowIds() {
    return maxRowIds;
  }

  public Authorizations getAuths() {
    return auths;
  }

  public String getIndexTable() {
    return indexTable;
  }

  public Set<String> getIndexColumns() {
    return indexColumns;
  }

  public Connector getConnect() {
    return connect;
  }
}
