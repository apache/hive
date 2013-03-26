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

package org.apache.hcatalog.utils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hcatalog.rcfile.RCFileInputDriver;
import org.apache.hcatalog.rcfile.RCFileOutputDriver;
import org.apache.thrift.TException;

/**
 * A utility program to annotate partitions of a pre-created table 
 * with input storage driver and output storage driver information
 */
public class PartitionStorageDriverAnnotator {

    /**
     * @param args
     * @throws MetaException 
     * @throws TException 
     * @throws NoSuchObjectException 
     * @throws InvalidOperationException 
     */
    public static void main(String[] args) throws MetaException, NoSuchObjectException, 
    TException, InvalidOperationException {
        String thrifturi = null;
        String database = "default";
        String table = null;
        String isd = null;
        String osd = null;
        Map<String, String> m = new HashMap<String, String>();
        for(int i = 0; i < args.length; i++) {
            if(args[i].equals("-u")) {
                thrifturi = args[i+1];
            } else if(args[i].equals("-t")) {
                table = args[i+1];
            } else if (args[i].equals("-i")) {
                isd = args[i+1];
            } else if (args[i].equals("-o")) {
                osd = args[i+1];
            } else if (args[i].equals("-p")) {
                String[] kvps = args[i+1].split(";");
                for(String kvp: kvps) {
                    String[] kv = kvp.split("=");
                    if(kv.length != 2) {
                        System.err.println("ERROR: key value property pairs must be specified as key1=val1;key2=val2;..;keyn=valn");
                        System.exit(1);
                    }
                    m.put(kv[0], kv[1]);
                }
            } else if(args[i].equals("-d")) {
                database = args[i+1];
            } else {
                System.err.println("ERROR: Unknown option: " + args[i]);
                usage();
            }
            i++; // to skip the value for an option
        }
        if(table == null || thrifturi == null) {
            System.err.println("ERROR: thrift uri and table name are mandatory");
            usage();
        }
        HiveConf hiveConf = new HiveConf(PartitionStorageDriverAnnotator.class);
        hiveConf.set("hive.metastore.local", "false");
        hiveConf.set("hive.metastore.uris", thrifturi);

        HiveMetaStoreClient hmsc = new HiveMetaStoreClient(hiveConf,null);
        List<Partition> parts = hmsc.listPartitions(database, table, Short.MAX_VALUE);
        
        m.put("hcat.isd", isd != null ? isd : RCFileInputDriver.class.getName());
        m.put("hcat.osd", osd != null ? osd : RCFileOutputDriver.class.getName()); 

        for(Partition p: parts) {
            p.setParameters(m);
            hmsc.alter_partition(database, table, p);
        }
    }

    /**
     * 
     */
    private static void usage() {
        System.err.println("Usage: java -cp testudf.jar:<hcatjar> org.apache.hcat.utils.PartitionStorageDriverAnnotator -u <thrift uri> -t <partitioned tablename>" +
        		" [-i input driver classname (Default rcfiledriver)] [-o output driver classname (default rcfiledriver)] " +
        		" [-p key1=val1;key2=val2;..;keyn=valn (list of key=value property pairs to associate with each partition)]" +
        		" [-d database (if this not supplied the default database is used)]");
        System.exit(1);
    }

}
