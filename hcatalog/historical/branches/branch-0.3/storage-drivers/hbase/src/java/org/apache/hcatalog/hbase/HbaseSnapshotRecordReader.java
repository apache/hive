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
package org.apache.hcatalog.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableRecordReader;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hcatalog.common.HCatUtil;
import org.apache.hcatalog.hbase.snapshot.TableSnapshot;
import org.apache.hcatalog.mapreduce.InputJobInfo;

/**
 * The Class HbaseSnapshotRecordReader implements logic for filtering records
 * based on snapshot.
 */
class HbaseSnapshotRecordReader extends TableRecordReader {

    static final Log LOG = LogFactory.getLog(HbaseSnapshotRecordReader.class);
    private ResultScanner scanner;
    private Scan  scan;
    private HTable  htable;
    private ImmutableBytesWritable key;
    private Result value;
    private InputJobInfo inpJobInfo;
    private TableSnapshot snapshot;
    private int maxRevisions;
    private Iterator<Result> resultItr;


    HbaseSnapshotRecordReader(InputJobInfo inputJobInfo) throws IOException {
        this.inpJobInfo = inputJobInfo;
        String snapshotString = inpJobInfo.getProperties().getProperty(
                HBaseConstants.PROPERTY_TABLE_SNAPSHOT_KEY);
        HCatTableSnapshot hcatSnapshot = (HCatTableSnapshot) HCatUtil
                .deserialize(snapshotString);
        this.snapshot = HBaseInputStorageDriver.convertSnapshot(hcatSnapshot,
                inpJobInfo.getTableInfo());
        this.maxRevisions = 1;
    }

    /* @param firstRow The first record in the split.
    /* @throws IOException
     * @see org.apache.hadoop.hbase.mapreduce.TableRecordReader#restart(byte[])
     */
    @Override
    public void restart(byte[] firstRow) throws IOException {
        Scan newScan = new Scan(scan);
        newScan.setStartRow(firstRow);
        this.scanner = this.htable.getScanner(newScan);
        resultItr = this.scanner.iterator();
    }

    /* @throws IOException
     * @see org.apache.hadoop.hbase.mapreduce.TableRecordReader#init()
     */
    @Override
    public void init() throws IOException {
        restart(scan.getStartRow());
    }

    /*
     * @param htable The HTable ( of HBase) to use for the record reader.
     *
     * @see
     * org.apache.hadoop.hbase.mapreduce.TableRecordReader#setHTable(org.apache
     * .hadoop.hbase.client.HTable)
     */
    @Override
    public void setHTable(HTable htable) {
        this.htable = htable;
    }

    /*
     * @param scan The scan to be used for reading records.
     *
     * @see
     * org.apache.hadoop.hbase.mapreduce.TableRecordReader#setScan(org.apache
     * .hadoop.hbase.client.Scan)
     */
    @Override
    public void setScan(Scan scan) {
        this.scan = scan;
    }

    /*
     * @see org.apache.hadoop.hbase.mapreduce.TableRecordReader#close()
     */
    @Override
    public void close() {
        this.resultItr = null;
        this.scanner.close();
    }

    /* @return The row of hbase record.
    /* @throws IOException
    /* @throws InterruptedException
     * @see org.apache.hadoop.hbase.mapreduce.TableRecordReader#getCurrentKey()
     */
    @Override
    public ImmutableBytesWritable getCurrentKey() throws IOException,
            InterruptedException {
        return key;
    }

    /* @return Single row result of scan of HBase table.
    /* @throws IOException
    /* @throws InterruptedException
     * @see org.apache.hadoop.hbase.mapreduce.TableRecordReader#getCurrentValue()
     */
    @Override
    public Result getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    /* @return Returns whether a next key-value is available for reading.
     * @see org.apache.hadoop.hbase.mapreduce.TableRecordReader#nextKeyValue()
     */
    @Override
    public boolean nextKeyValue() {

        if (this.resultItr == null) {
            LOG.warn("The HBase result iterator is found null. It is possible"
                    + " that the record reader has already been closed.");
        } else {

            if (key == null)
                key = new ImmutableBytesWritable();
            while (resultItr.hasNext()) {
                Result temp = resultItr.next();
                Result hbaseRow = prepareResult(temp.list());
                if (hbaseRow != null) {
                    key.set(hbaseRow.getRow());
                    value = hbaseRow;
                    return true;
                }

            }
        }
        return false;
    }

    private Result prepareResult(List<KeyValue> keyvalues) {

        List<KeyValue> finalKeyVals = new ArrayList<KeyValue>();
        Map<String, List<KeyValue>> qualValMap = new HashMap<String, List<KeyValue>>();
        for (KeyValue kv : keyvalues) {
            byte[] cf = kv.getFamily();
            byte[] qualifier = kv.getQualifier();
            String key = Bytes.toString(cf) + ":" + Bytes.toString(qualifier);
            List<KeyValue> kvs;
            if (qualValMap.containsKey(key)) {
                kvs = qualValMap.get(key);
            } else {
                kvs = new ArrayList<KeyValue>();
            }

            String family = Bytes.toString(kv.getFamily());
            long desiredTS = snapshot.getRevision(family);
            if (kv.getTimestamp() <= desiredTS) {
                kvs.add(kv);
            }
            qualValMap.put(key, kvs);
        }

        Set<String> keys = qualValMap.keySet();
        for (String cf : keys) {
            List<KeyValue> kvs = qualValMap.get(cf);
            if (maxRevisions <= kvs.size()) {
                for (int i = 0; i < maxRevisions; i++) {
                    finalKeyVals.add(kvs.get(i));
                }
            } else {
                finalKeyVals.addAll(kvs);
            }
        }

        if(finalKeyVals.size() == 0){
            return null;
        } else {
            KeyValue[] kvArray = new KeyValue[finalKeyVals.size()];
            finalKeyVals.toArray(kvArray);
            return new Result(kvArray);
        }
    }

    /* @return The progress of the record reader.
     * @see org.apache.hadoop.hbase.mapreduce.TableRecordReader#getProgress()
     */
    @Override
    public float getProgress() {
        // Depends on the total number of tuples
        return 0;
    }

}
