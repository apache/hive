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
package org.apache.hcatalog.pig;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hcatalog.common.HCatConstants;
import org.apache.hcatalog.common.HCatException;
import org.apache.hcatalog.common.HCatUtil;
import org.apache.hcatalog.data.HCatRecord;
import org.apache.hcatalog.data.Pair;
import org.apache.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hcatalog.data.schema.HCatFieldSchema.Type;
import org.apache.hcatalog.data.schema.HCatSchema;
import org.apache.pig.LoadPushDown.RequiredField;
import org.apache.pig.PigException;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.DefaultDataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.impl.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PigHCatUtil {

    private static final Logger LOG = LoggerFactory.getLogger(PigHCatUtil.class);

    static final int PIG_EXCEPTION_CODE = 1115; // http://wiki.apache.org/pig/PigErrorHandlingFunctionalSpecification#Error_codes
    private static final String DEFAULT_DB = MetaStoreUtils.DEFAULT_DATABASE_NAME;

    private final Map<Pair<String, String>, Table> hcatTableCache =
        new HashMap<Pair<String, String>, Table>();

    private static final TupleFactory tupFac = TupleFactory.getInstance();

    private static boolean pigHasBooleanSupport = false;

    /**
     * Determine if the current Pig version supports boolean columns. This works around a
     * dependency conflict preventing HCatalog from requiring a version of Pig with boolean
     * field support and should be removed once HCATALOG-466 has been resolved.
     */
    static {
        // DETAILS:
        //
        // PIG-1429 added support for boolean fields, which shipped in 0.10.0;
        // this version of Pig depends on antlr 3.4.
        //
        // HCatalog depends heavily on Hive, which at this time uses antlr 3.0.1.
        //
        // antlr 3.0.1 and 3.4 are incompatible, so Pig 0.10.0 and Hive cannot be depended on in the
        // same project. Pig 0.8.0 did not use antlr for its parser and can coexist with Hive,
        // so that Pig version is depended on by HCatalog at this time.
        try {
            Schema schema = Utils.getSchemaFromString("myBooleanField: boolean");
            pigHasBooleanSupport = (schema.getField("myBooleanField").type == DataType.BOOLEAN);
        } catch (Throwable e) {
            // pass
        }

        if (!pigHasBooleanSupport) {
            LOG.info("This version of Pig does not support boolean fields. To enable "
                    + "boolean-to-integer conversion, set the "
                    + HCatConstants.HCAT_DATA_CONVERT_BOOLEAN_TO_INTEGER
                    + "=true configuration parameter.");
        }
    }

    static public Pair<String, String> getDBTableNames(String location) throws IOException {
        // the location string will be of the form:
        // <database name>.<table name> - parse it and
        // communicate the information to HCatInputFormat

        try {
            return HCatUtil.getDbAndTableName(location);
        } catch (IOException e) {
            String locationErrMsg = "The input location in load statement " +
                "should be of the form " +
                "<databasename>.<table name> or <table name>. Got " + location;
            throw new PigException(locationErrMsg, PIG_EXCEPTION_CODE);
        }
    }

    static public String getHCatServerUri(Job job) {

        return job.getConfiguration().get(HiveConf.ConfVars.METASTOREURIS.varname);
    }

    static public String getHCatServerPrincipal(Job job) {

        return job.getConfiguration().get(HCatConstants.HCAT_METASTORE_PRINCIPAL);
    }

    private static HiveMetaStoreClient getHiveMetaClient(String serverUri,
                                                         String serverKerberosPrincipal, Class<?> clazz) throws Exception {
        HiveConf hiveConf = new HiveConf(clazz);

        if (serverUri != null) {
            hiveConf.set("hive.metastore.local", "false");
            hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, serverUri.trim());
        }

        if (serverKerberosPrincipal != null) {
            hiveConf.setBoolVar(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL, true);
            hiveConf.setVar(HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL, serverKerberosPrincipal);
        }

        try {
            return HCatUtil.getHiveClient(hiveConf);
        } catch (Exception e) {
            throw new Exception("Could not instantiate a HiveMetaStoreClient connecting to server uri:[" + serverUri + "]", e);
        }
    }


    HCatSchema getHCatSchema(List<RequiredField> fields, String signature, Class<?> classForUDFCLookup) throws IOException {
        if (fields == null) {
            return null;
        }

        Properties props = UDFContext.getUDFContext().getUDFProperties(
            classForUDFCLookup, new String[]{signature});
        HCatSchema hcatTableSchema = (HCatSchema) props.get(HCatConstants.HCAT_TABLE_SCHEMA);

        ArrayList<HCatFieldSchema> fcols = new ArrayList<HCatFieldSchema>();
        for (RequiredField rf : fields) {
            fcols.add(hcatTableSchema.getFields().get(rf.getIndex()));
        }
        return new HCatSchema(fcols);
    }

    public Table getTable(String location, String hcatServerUri, String hcatServerPrincipal) throws IOException {
        Pair<String, String> loc_server = new Pair<String, String>(location, hcatServerUri);
        Table hcatTable = hcatTableCache.get(loc_server);
        if (hcatTable != null) {
            return hcatTable;
        }

        Pair<String, String> dbTablePair = PigHCatUtil.getDBTableNames(location);
        String dbName = dbTablePair.first;
        String tableName = dbTablePair.second;
        Table table = null;
        HiveMetaStoreClient client = null;
        try {
            client = getHiveMetaClient(hcatServerUri, hcatServerPrincipal, PigHCatUtil.class);
            table = HCatUtil.getTable(client, dbName, tableName);
        } catch (NoSuchObjectException nsoe) {
            throw new PigException("Table not found : " + nsoe.getMessage(), PIG_EXCEPTION_CODE); // prettier error messages to frontend
        } catch (Exception e) {
            throw new IOException(e);
        } finally {
            HCatUtil.closeHiveClientQuietly(client);
        }
        hcatTableCache.put(loc_server, table);
        return table;
    }

    public static ResourceSchema getResourceSchema(HCatSchema hcatSchema) throws IOException {

        List<ResourceFieldSchema> rfSchemaList = new ArrayList<ResourceFieldSchema>();
        for (HCatFieldSchema hfs : hcatSchema.getFields()) {
            ResourceFieldSchema rfSchema;
            rfSchema = getResourceSchemaFromFieldSchema(hfs);
            rfSchemaList.add(rfSchema);
        }
        ResourceSchema rSchema = new ResourceSchema();
        rSchema.setFields(rfSchemaList.toArray(new ResourceFieldSchema[0]));
        return rSchema;

    }

    private static ResourceFieldSchema getResourceSchemaFromFieldSchema(HCatFieldSchema hfs)
        throws IOException {
        ResourceFieldSchema rfSchema;
        // if we are dealing with a bag or tuple column - need to worry about subschema
        if (hfs.getType() == Type.STRUCT) {
            rfSchema = new ResourceFieldSchema()
                .setName(hfs.getName())
                .setDescription(hfs.getComment())
                .setType(getPigType(hfs))
                .setSchema(getTupleSubSchema(hfs));
        } else if (hfs.getType() == Type.ARRAY) {
            rfSchema = new ResourceFieldSchema()
                .setName(hfs.getName())
                .setDescription(hfs.getComment())
                .setType(getPigType(hfs))
                .setSchema(getBagSubSchema(hfs));
        } else {
            rfSchema = new ResourceFieldSchema()
                .setName(hfs.getName())
                .setDescription(hfs.getComment())
                .setType(getPigType(hfs))
                .setSchema(null); // no munging inner-schemas
        }
        return rfSchema;
    }

    protected static ResourceSchema getBagSubSchema(HCatFieldSchema hfs) throws IOException {
        // there are two cases - array<Type> and array<struct<...>>
        // in either case the element type of the array is represented in a
        // tuple field schema in the bag's field schema - the second case (struct)
        // more naturally translates to the tuple - in the first case (array<Type>)
        // we simulate the tuple by putting the single field in a tuple

        Properties props = UDFContext.getUDFContext().getClientSystemProps();
        String innerTupleName = HCatConstants.HCAT_PIG_INNER_TUPLE_NAME_DEFAULT;
        if (props != null && props.containsKey(HCatConstants.HCAT_PIG_INNER_TUPLE_NAME)) {
            innerTupleName = props.getProperty(HCatConstants.HCAT_PIG_INNER_TUPLE_NAME)
                .replaceAll("FIELDNAME", hfs.getName());
        }
        String innerFieldName = HCatConstants.HCAT_PIG_INNER_FIELD_NAME_DEFAULT;
        if (props != null && props.containsKey(HCatConstants.HCAT_PIG_INNER_FIELD_NAME)) {
            innerFieldName = props.getProperty(HCatConstants.HCAT_PIG_INNER_FIELD_NAME)
                .replaceAll("FIELDNAME", hfs.getName());
        }

        ResourceFieldSchema[] bagSubFieldSchemas = new ResourceFieldSchema[1];
        bagSubFieldSchemas[0] = new ResourceFieldSchema().setName(innerTupleName)
            .setDescription("The tuple in the bag")
            .setType(DataType.TUPLE);
        HCatFieldSchema arrayElementFieldSchema = hfs.getArrayElementSchema().get(0);
        if (arrayElementFieldSchema.getType() == Type.STRUCT) {
            bagSubFieldSchemas[0].setSchema(getTupleSubSchema(arrayElementFieldSchema));
        } else if (arrayElementFieldSchema.getType() == Type.ARRAY) {
            ResourceSchema s = new ResourceSchema();
            List<ResourceFieldSchema> lrfs = Arrays.asList(getResourceSchemaFromFieldSchema(arrayElementFieldSchema));
            s.setFields(lrfs.toArray(new ResourceFieldSchema[0]));
            bagSubFieldSchemas[0].setSchema(s);
        } else {
            ResourceFieldSchema[] innerTupleFieldSchemas = new ResourceFieldSchema[1];
            innerTupleFieldSchemas[0] = new ResourceFieldSchema().setName(innerFieldName)
                .setDescription("The inner field in the tuple in the bag")
                .setType(getPigType(arrayElementFieldSchema))
                .setSchema(null); // the element type is not a tuple - so no subschema
            bagSubFieldSchemas[0].setSchema(new ResourceSchema().setFields(innerTupleFieldSchemas));
        }
        ResourceSchema s = new ResourceSchema().setFields(bagSubFieldSchemas);
        return s;

    }

    private static ResourceSchema getTupleSubSchema(HCatFieldSchema hfs) throws IOException {
        // for each struct subfield, create equivalent ResourceFieldSchema
        ResourceSchema s = new ResourceSchema();
        List<ResourceFieldSchema> lrfs = new ArrayList<ResourceFieldSchema>();
        for (HCatFieldSchema subField : hfs.getStructSubSchema().getFields()) {
            lrfs.add(getResourceSchemaFromFieldSchema(subField));
        }
        s.setFields(lrfs.toArray(new ResourceFieldSchema[0]));
        return s;
    }

    /**
     * @param hfs the field schema of the column
     * @return corresponding pig type
     * @throws IOException
     */
    static public byte getPigType(HCatFieldSchema hfs) throws IOException {
        return getPigType(hfs.getType());
    }

    static public byte getPigType(Type type) throws IOException {
        if (type == Type.STRING) {
            return DataType.CHARARRAY;
        }

        if ((type == Type.INT) || (type == Type.SMALLINT) || (type == Type.TINYINT)) {
            return DataType.INTEGER;
        }

        if (type == Type.ARRAY) {
            return DataType.BAG;
        }

        if (type == Type.STRUCT) {
            return DataType.TUPLE;
        }

        if (type == Type.MAP) {
            return DataType.MAP;
        }

        if (type == Type.BIGINT) {
            return DataType.LONG;
        }

        if (type == Type.FLOAT) {
            return DataType.FLOAT;
        }

        if (type == Type.DOUBLE) {
            return DataType.DOUBLE;
        }

        if (type == Type.BINARY) {
            return DataType.BYTEARRAY;
        }

        if (type == Type.BOOLEAN && pigHasBooleanSupport) {
            return DataType.BOOLEAN;
        }

        throw new PigException("HCatalog column type '" + type.toString()
                + "' is not supported in Pig as a column type", PIG_EXCEPTION_CODE);
    }

    public static Tuple transformToTuple(HCatRecord hr, HCatSchema hs) throws Exception {
        if (hr == null) {
            return null;
        }
        return transformToTuple(hr.getAll(), hs);
    }

    @SuppressWarnings("unchecked")
    public static Object extractPigObject(Object o, HCatFieldSchema hfs) throws Exception {
        Object result;
        Type itemType = hfs.getType();
        switch (itemType) {
        case BINARY:
            result = (o == null) ? null : new DataByteArray((byte[]) o);
            break;
        case STRUCT:
            result = transformToTuple((List<Object>) o, hfs);
            break;
        case ARRAY:
            result = transformToBag((List<? extends Object>) o, hfs);
            break;
        case MAP:
            result = transformToPigMap((Map<Object, Object>) o, hfs);
            break;
        default:
            result = o;
            break;
        }
        return result;
    }

    private static Tuple transformToTuple(List<? extends Object> objList, HCatFieldSchema hfs) throws Exception {
        try {
            return transformToTuple(objList, hfs.getStructSubSchema());
        } catch (Exception e) {
            if (hfs.getType() != Type.STRUCT) {
                throw new Exception("Expected Struct type, got " + hfs.getType(), e);
            } else {
                throw e;
            }
        }
    }

    private static Tuple transformToTuple(List<? extends Object> objList, HCatSchema hs) throws Exception {
        if (objList == null) {
            return null;
        }
        Tuple t = tupFac.newTuple(objList.size());
        List<HCatFieldSchema> subFields = hs.getFields();
        for (int i = 0; i < subFields.size(); i++) {
            t.set(i, extractPigObject(objList.get(i), subFields.get(i)));
        }
        return t;
    }

    private static Map<String, Object> transformToPigMap(Map<Object, Object> map, HCatFieldSchema hfs) throws Exception {
        if (map == null) {
            return null;
        }

        Map<String, Object> result = new HashMap<String, Object>();
        for (Entry<Object, Object> entry : map.entrySet()) {
            // since map key for Pig has to be Strings
            result.put(entry.getKey().toString(), extractPigObject(entry.getValue(), hfs.getMapValueSchema().get(0)));
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private static DataBag transformToBag(List<? extends Object> list, HCatFieldSchema hfs) throws Exception {
        if (list == null) {
            return null;
        }

        HCatFieldSchema elementSubFieldSchema = hfs.getArrayElementSchema().getFields().get(0);
        DataBag db = new DefaultDataBag();
        for (Object o : list) {
            Tuple tuple;
            if (elementSubFieldSchema.getType() == Type.STRUCT) {
                tuple = transformToTuple((List<Object>) o, elementSubFieldSchema);
            } else {
                // bags always contain tuples
                tuple = tupFac.newTuple(extractPigObject(o, elementSubFieldSchema));
            }
            db.add(tuple);
        }
        return db;
    }


    private static void validateHCatSchemaFollowsPigRules(HCatSchema tblSchema) throws PigException {
        for (HCatFieldSchema hcatField : tblSchema.getFields()) {
            validateHcatFieldFollowsPigRules(hcatField);
        }
    }

    private static void validateHcatFieldFollowsPigRules(HCatFieldSchema hcatField) throws PigException {
        try {
            Type hType = hcatField.getType();
            switch (hType) {
            case BOOLEAN:
                if (!pigHasBooleanSupport) {
                    throw new PigException("Incompatible type found in HCat table schema: "
                            + hcatField, PigHCatUtil.PIG_EXCEPTION_CODE);
                }
                break;
            case ARRAY:
                validateHCatSchemaFollowsPigRules(hcatField.getArrayElementSchema());
                break;
            case STRUCT:
                validateHCatSchemaFollowsPigRules(hcatField.getStructSubSchema());
                break;
            case MAP:
                // key is only string
                if (hcatField.getMapKeyType() != Type.STRING) {
                    LOG.info("Converting non-String key of map " + hcatField.getName() + " from "
                        + hcatField.getMapKeyType() + " to String.");
                }
                validateHCatSchemaFollowsPigRules(hcatField.getMapValueSchema());
                break;
            }
        } catch (HCatException e) {
            throw new PigException("Incompatible type found in hcat table schema: " + hcatField, PigHCatUtil.PIG_EXCEPTION_CODE, e);
        }
    }


    public static void validateHCatTableSchemaFollowsPigRules(HCatSchema hcatTableSchema) throws IOException {
        validateHCatSchemaFollowsPigRules(hcatTableSchema);
    }

    public static void getConfigFromUDFProperties(Properties p, Configuration config, String propName) {
        if (p.getProperty(propName) != null) {
            config.set(propName, p.getProperty(propName));
        }
    }

    public static void saveConfigIntoUDFProperties(Properties p, Configuration config, String propName) {
        if (config.get(propName) != null) {
            p.setProperty(propName, config.get(propName));
        }
    }

}
