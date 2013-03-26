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

package org.apache.hcatalog.mapreduce;

import java.io.Serializable;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hcatalog.data.schema.HCatSchema;

/** The class used to serialize and store the output related information  */
class OutputJobInfo implements Serializable {

    /** The serialization version. */
    private static final long serialVersionUID = 1L;

    /** The table info provided by user. */
    private final HCatTableInfo tableInfo;

    /** The output schema. This is given to us by user.  This wont contain any
     * partition columns ,even if user has specified them.
     * */
    private HCatSchema outputSchema;

    /** This is table schema, retrieved from metastore. */
    private final HCatSchema tableSchema;

    /** The storer info */
    private final StorerInfo storerInfo;

    /** The location of the partition being written */
    private final String location;

    /** The table being written to */
    private final Table table;

    /** This is a list of partition columns which will be deleted from data, if
     * data contains partition columns.*/

    private List<Integer> posOfPartCols;

    /**
     * @return the posOfPartCols
     */
    protected List<Integer> getPosOfPartCols() {
      return posOfPartCols;
    }

    /**
     * @param posOfPartCols the posOfPartCols to set
     */
    protected void setPosOfPartCols(List<Integer> posOfPartCols) {
      // sorting the list in the descending order so that deletes happen back-to-front
      Collections.sort(posOfPartCols, new Comparator<Integer> () {
        @Override
        public int compare(Integer earlier, Integer later) {
          return (earlier > later) ? -1 : ((earlier == later) ? 0 : 1);
        }
      });
      this.posOfPartCols = posOfPartCols;
    }

    public OutputJobInfo(HCatTableInfo tableInfo, HCatSchema outputSchema, HCatSchema tableSchema,
        StorerInfo storerInfo, String location, Table table) {
      super();
      this.tableInfo = tableInfo;
      this.outputSchema = outputSchema;
      this.tableSchema = tableSchema;
      this.storerInfo = storerInfo;
      this.location = location;
      this.table = table;
    }

    /**
     * @return the tableInfo
     */
    public HCatTableInfo getTableInfo() {
      return tableInfo;
    }

    /**
     * @return the outputSchema
     */
    public HCatSchema getOutputSchema() {
      return outputSchema;
    }

    /**
     * @param schema the outputSchema to set
     */
    public void setOutputSchema(HCatSchema schema) {
      this.outputSchema = schema;
    }

    /**
     * @return the tableSchema
     */
    public HCatSchema getTableSchema() {
      return tableSchema;
    }

    /**
     * @return the storerInfo
     */
    public StorerInfo getStorerInfo() {
      return storerInfo;
    }

    /**
     * @return the location
     */
    public String getLocation() {
      return location;
    }

    /**
     * Gets the value of table
     * @return the table
     */
    public Table getTable() {
      return table;
    }

}
