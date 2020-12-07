package org.apache.hadoop.hive.metastore.client.builder;

/**
 * Builder for building table capabilities to be includes in TBLPROPERTIES
 * during createTable
 */
public class TableCapabilityBuilder {
    private String capabilitiesString = null;
    public static final String KEY_CAPABILITIES = "OBJCAPABILITIES";

    public TableCapabilityBuilder() {
        capabilitiesString = "";
    }

    public TableCapabilityBuilder add(String skill) {
        if (skill != null) {
            capabilitiesString += skill + ",";
        }
        return this;
    }

    public String build() {
        return this.capabilitiesString.substring(0, capabilitiesString.length() - 1);
    }

    public String getDBValue() {
        return KEY_CAPABILITIES + "=" + build();
    }

    public static String getKey() {
        return KEY_CAPABILITIES;
    }
}
