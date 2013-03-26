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

package org.apache.hcatalog.common;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.thrift.DelegationTokenIdentifier;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hcatalog.data.Pair;
import org.apache.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hcatalog.data.schema.HCatSchema;
import org.apache.hcatalog.data.schema.HCatSchemaUtils;
import org.apache.hcatalog.mapreduce.HCatOutputFormat;
import org.apache.hcatalog.storagehandler.HCatStorageHandler;
import org.apache.thrift.TException;

public class HCatUtil {

    // static final private Log LOG = LogFactory.getLog(HCatUtil.class);

    public static boolean checkJobContextIfRunningFromBackend(JobContext j) {
        if (j.getConfiguration().get("mapred.task.id", "").equals("")) {
            return false;
        }
        return true;
    }

    public static String serialize(Serializable obj) throws IOException {
        if (obj == null) {
            return "";
        }
        try {
            ByteArrayOutputStream serialObj = new ByteArrayOutputStream();
            ObjectOutputStream objStream = new ObjectOutputStream(serialObj);
            objStream.writeObject(obj);
            objStream.close();
            return encodeBytes(serialObj.toByteArray());
        } catch (Exception e) {
            throw new IOException("Serialization error: " + e.getMessage(), e);
        }
    }

    public static Object deserialize(String str) throws IOException {
        if (str == null || str.length() == 0) {
            return null;
        }
        try {
            ByteArrayInputStream serialObj = new ByteArrayInputStream(
                    decodeBytes(str));
            ObjectInputStream objStream = new ObjectInputStream(serialObj);
            return objStream.readObject();
        } catch (Exception e) {
            throw new IOException("Deserialization error: " + e.getMessage(), e);
        }
    }

    public static String encodeBytes(byte[] bytes) {
        StringBuffer strBuf = new StringBuffer();

        for (int i = 0; i < bytes.length; i++) {
            strBuf.append((char) (((bytes[i] >> 4) & 0xF) + ('a')));
            strBuf.append((char) (((bytes[i]) & 0xF) + ('a')));
        }

        return strBuf.toString();
    }

    public static byte[] decodeBytes(String str) {
        byte[] bytes = new byte[str.length() / 2];
        for (int i = 0; i < str.length(); i += 2) {
            char c = str.charAt(i);
            bytes[i / 2] = (byte) ((c - 'a') << 4);
            c = str.charAt(i + 1);
            bytes[i / 2] += (c - 'a');
        }
        return bytes;
    }

    public static List<HCatFieldSchema> getHCatFieldSchemaList(
            FieldSchema... fields) throws HCatException {
        List<HCatFieldSchema> result = new ArrayList<HCatFieldSchema>(
                fields.length);

        for (FieldSchema f : fields) {
            result.add(HCatSchemaUtils.getHCatFieldSchema(f));
        }

        return result;
    }

    public static List<HCatFieldSchema> getHCatFieldSchemaList(
            List<FieldSchema> fields) throws HCatException {
        if (fields == null) {
            return null;
        } else {
            List<HCatFieldSchema> result = new ArrayList<HCatFieldSchema>();
            for (FieldSchema f : fields) {
                result.add(HCatSchemaUtils.getHCatFieldSchema(f));
            }
            return result;
        }
    }

    public static HCatSchema extractSchemaFromStorageDescriptor(
            StorageDescriptor sd) throws HCatException {
        if (sd == null) {
            throw new HCatException(
                    "Cannot construct partition info from an empty storage descriptor.");
        }
        HCatSchema schema = new HCatSchema(HCatUtil.getHCatFieldSchemaList(sd
                .getCols()));
        return schema;
    }

    public static List<FieldSchema> getFieldSchemaList(
            List<HCatFieldSchema> hcatFields) {
        if (hcatFields == null) {
            return null;
        } else {
            List<FieldSchema> result = new ArrayList<FieldSchema>();
            for (HCatFieldSchema f : hcatFields) {
                result.add(HCatSchemaUtils.getFieldSchema(f));
            }
            return result;
        }
    }

    public static Table getTable(HiveMetaStoreClient client, String dbName,
            String tableName) throws Exception {
        return client.getTable(dbName, tableName);
    }

    public static HCatSchema getTableSchemaWithPtnCols(Table table)
            throws IOException {
        HCatSchema tableSchema = extractSchemaFromStorageDescriptor(table
                .getSd());

        if (table.getPartitionKeys().size() != 0) {

            // add partition keys to table schema
            // NOTE : this assumes that we do not ever have ptn keys as columns
            // inside the table schema as well!
            for (FieldSchema fs : table.getPartitionKeys()) {
                tableSchema.append(HCatSchemaUtils.getHCatFieldSchema(fs));
            }
        }
        return tableSchema;
    }

    /**
     * return the partition columns from a table instance
     *
     * @param table the instance to extract partition columns from
     * @return HCatSchema instance which contains the partition columns
     * @throws IOException
     */
    public static HCatSchema getPartitionColumns(Table table)
            throws IOException {
        HCatSchema cols = new HCatSchema(new LinkedList<HCatFieldSchema>());
        if (table.getPartitionKeys().size() != 0) {
            for (FieldSchema fs : table.getPartitionKeys()) {
                cols.append(HCatSchemaUtils.getHCatFieldSchema(fs));
            }
        }
        return cols;
    }

    /**
     * Validate partition schema, checks if the column types match between the
     * partition and the existing table schema. Returns the list of columns
     * present in the partition but not in the table.
     *
     * @param table the table
     * @param partitionSchema the partition schema
     * @return the list of newly added fields
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public static List<FieldSchema> validatePartitionSchema(Table table,
            HCatSchema partitionSchema) throws IOException {
        Map<String, FieldSchema> partitionKeyMap = new HashMap<String, FieldSchema>();

        for (FieldSchema field : table.getPartitionKeys()) {
            partitionKeyMap.put(field.getName().toLowerCase(), field);
        }

        List<FieldSchema> tableCols = table.getSd().getCols();
        List<FieldSchema> newFields = new ArrayList<FieldSchema>();

        for (int i = 0; i < partitionSchema.getFields().size(); i++) {

            FieldSchema field = HCatSchemaUtils.getFieldSchema(partitionSchema
                    .getFields().get(i));

            FieldSchema tableField;
            if (i < tableCols.size()) {
                tableField = tableCols.get(i);

                if (!tableField.getName().equalsIgnoreCase(field.getName())) {
                    throw new HCatException(
                            ErrorType.ERROR_SCHEMA_COLUMN_MISMATCH,
                            "Expected column <" + tableField.getName()
                                    + "> at position " + (i + 1)
                                    + ", found column <" + field.getName()
                                    + ">");
                }
            } else {
                tableField = partitionKeyMap.get(field.getName().toLowerCase());

                if (tableField != null) {
                    throw new HCatException(
                            ErrorType.ERROR_SCHEMA_PARTITION_KEY, "Key <"
                                    + field.getName() + ">");
                }
            }

            if (tableField == null) {
                // field present in partition but not in table
                newFields.add(field);
            } else {
                // field present in both. validate type has not changed
                TypeInfo partitionType = TypeInfoUtils
                        .getTypeInfoFromTypeString(field.getType());
                TypeInfo tableType = TypeInfoUtils
                        .getTypeInfoFromTypeString(tableField.getType());

                if (!partitionType.equals(tableType)) {
                    throw new HCatException(
                            ErrorType.ERROR_SCHEMA_TYPE_MISMATCH, "Column <"
                                    + field.getName() + ">, expected <"
                                    + tableType.getTypeName() + ">, got <"
                                    + partitionType.getTypeName() + ">");
                }
            }
        }

        return newFields;
    }

    /**
     * Test if the first FsAction is more permissive than the second. This is
     * useful in cases where we want to ensure that a file owner has more
     * permissions than the group they belong to, for eg. More completely(but
     * potentially more cryptically) owner-r >= group-r >= world-r : bitwise
     * and-masked with 0444 => 444 >= 440 >= 400 >= 000 owner-w >= group-w >=
     * world-w : bitwise and-masked with &0222 => 222 >= 220 >= 200 >= 000
     * owner-x >= group-x >= world-x : bitwise and-masked with &0111 => 111 >=
     * 110 >= 100 >= 000
     *
     * @return true if first FsAction is more permissive than the second, false
     *         if not.
     */
    public static boolean validateMorePermissive(FsAction first, FsAction second) {
        if ((first == FsAction.ALL) || (second == FsAction.NONE)
                || (first == second)) {
            return true;
        }
        switch (first) {
            case READ_EXECUTE:
                return ((second == FsAction.READ) || (second == FsAction.EXECUTE));
            case READ_WRITE:
                return ((second == FsAction.READ) || (second == FsAction.WRITE));
            case WRITE_EXECUTE:
                return ((second == FsAction.WRITE) || (second == FsAction.EXECUTE));
        }
        return false;
    }

    /**
     * Ensure that read or write permissions are not granted without also
     * granting execute permissions. Essentially, r-- , rw- and -w- are invalid,
     * r-x, -wx, rwx, ---, --x are valid
     *
     * @param perms The FsAction to verify
     * @return true if the presence of read or write permission is accompanied
     *         by execute permissions
     */
    public static boolean validateExecuteBitPresentIfReadOrWrite(FsAction perms) {
        if ((perms == FsAction.READ) || (perms == FsAction.WRITE)
                || (perms == FsAction.READ_WRITE)) {
            return false;
        }
        return true;
    }

    public static Token<org.apache.hadoop.mapreduce.security.token.delegation.DelegationTokenIdentifier> getJobTrackerDelegationToken(
            Configuration conf, String userName) throws Exception {
        // LOG.info("getJobTrackerDelegationToken("+conf+","+userName+")");
        JobClient jcl = new JobClient(new JobConf(conf, HCatOutputFormat.class));
        Token<org.apache.hadoop.mapreduce.security.token.delegation.DelegationTokenIdentifier> t = jcl
                .getDelegationToken(new Text(userName));
        // LOG.info("got "+t);
        return t;

        // return null;
    }

    public static void cancelJobTrackerDelegationToken(String tokenStrForm,
            String tokenSignature) throws Exception {
        // LOG.info("cancelJobTrackerDelegationToken("+tokenStrForm+","+tokenSignature+")");
        JobClient jcl = new JobClient(new JobConf(new Configuration(),
                HCatOutputFormat.class));
        Token<org.apache.hadoop.mapreduce.security.token.delegation.DelegationTokenIdentifier> t = extractJobTrackerToken(
                tokenStrForm, tokenSignature);
        // LOG.info("canceling "+t);
        try {
            jcl.cancelDelegationToken(t);
        } catch (Exception e) {
            // HCatUtil.logToken(LOG, "jcl token to cancel", t);
            // ignore if token has already been invalidated.
        }
    }

    public static Token<? extends AbstractDelegationTokenIdentifier> extractThriftToken(
            String tokenStrForm, String tokenSignature) throws MetaException,
            TException, IOException {
        // LOG.info("extractThriftToken("+tokenStrForm+","+tokenSignature+")");
        Token<? extends AbstractDelegationTokenIdentifier> t = new Token<DelegationTokenIdentifier>();
        t.decodeFromUrlString(tokenStrForm);
        t.setService(new Text(tokenSignature));
        // LOG.info("returning "+t);
        return t;
    }

    public static Token<org.apache.hadoop.mapreduce.security.token.delegation.DelegationTokenIdentifier> extractJobTrackerToken(
            String tokenStrForm, String tokenSignature) throws MetaException,
            TException, IOException {
        // LOG.info("extractJobTrackerToken("+tokenStrForm+","+tokenSignature+")");
        Token<org.apache.hadoop.mapreduce.security.token.delegation.DelegationTokenIdentifier> t = new Token<org.apache.hadoop.mapreduce.security.token.delegation.DelegationTokenIdentifier>();
        t.decodeFromUrlString(tokenStrForm);
        t.setService(new Text(tokenSignature));
        // LOG.info("returning "+t);
        return t;
    }

    /**
     * Logging stack trace
     *
     * @param logger
     */
    public static void logStackTrace(Log logger) {
        StackTraceElement[] stackTrace = new Exception().getStackTrace();
        for (int i = 1; i < stackTrace.length; i++) {
            logger.info("\t" + stackTrace[i].toString());
        }
    }

    /**
     * debug log the hive conf
     *
     * @param logger
     * @param hc
     */
    public static void logHiveConf(Log logger, HiveConf hc) {
        logEntrySet(logger, "logging hiveconf:", hc.getAllProperties()
                .entrySet());
    }

    public static void logList(Log logger, String itemName,
            List<? extends Object> list) {
        logger.info(itemName + ":");
        for (Object item : list) {
            logger.info("\t[" + item + "]");
        }
    }

    public static void logMap(Log logger, String itemName,
            Map<? extends Object, ? extends Object> map) {
        logEntrySet(logger, itemName, map.entrySet());
    }

    public static void logEntrySet(Log logger, String itemName,
            Set<? extends Entry> entrySet) {
        logIterableSet(logger,itemName,entrySet.iterator());
    }

    public static void logIterableSet(Log logger, String itemName, Iterator<? extends Entry> iterator){
      logger.info(itemName + ":");
      while (iterator.hasNext()){
        Entry e = iterator.next();
        logger.debug("\t[" + e.getKey() + "]=>[" + e.getValue() + "]");
      }
    }
    
    public static void logAllTokens(Log logger, JobContext context)
            throws IOException {
        for (Token<? extends TokenIdentifier> t : context.getCredentials()
                .getAllTokens()) {
            logToken(logger, "token", t);
        }
    }

    public static void logToken(Log logger, String itemName,
            Token<? extends TokenIdentifier> t) throws IOException {
        logger.info(itemName + ":");
        logger.info("\tencodeToUrlString : " + t.encodeToUrlString());
        logger.info("\ttoString : " + t.toString());
        logger.info("\tkind : " + t.getKind());
        logger.info("\tservice : " + t.getService());
    }

    public static HCatStorageHandler getStorageHandler(Configuration conf,
            String className) throws HiveException {

        if (className == null) {
            return null;
        }
        try {
            Class<? extends HCatStorageHandler> handlerClass = (Class<? extends HCatStorageHandler>) Class
                    .forName(className, true, JavaUtils.getClassLoader());
            HCatStorageHandler storageHandler = (HCatStorageHandler) ReflectionUtils
                    .newInstance(handlerClass, conf);
            return storageHandler;
        } catch (ClassNotFoundException e) {
            throw new HiveException("Error in loading storage handler."
                    + e.getMessage(), e);
        }
    }

    public static Pair<String,String> getDbAndTableName(String tableName) throws IOException{
      String[] dbTableNametokens = tableName.split("\\.");
      if(dbTableNametokens.length == 1) {
        return new Pair<String,String>(MetaStoreUtils.DEFAULT_DATABASE_NAME,tableName);
      }else if (dbTableNametokens.length == 2) {
        return new Pair<String, String>(dbTableNametokens[0], dbTableNametokens[1]);
      }else{
        throw new IOException("tableName expected in the form "
            +"<databasename>.<table name> or <table name>. Got " + tableName);
      }
    }
}
