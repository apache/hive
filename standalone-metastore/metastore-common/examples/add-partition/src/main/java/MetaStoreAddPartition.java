import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.thrift.TException;

public class MetaStoreAddPartition {


    public static void main(String[] args) {
        //ex: "thrift://localhost:9083"
        String metaStoreUri = args[0];
        HiveConf hiveConf = new HiveConf(MetaStoreAddPartition.class);
        hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, metaStoreUri);
        try {
            HiveMetaStoreClient client = new HiveMetaStoreClient(hiveConf);
            String dbName = "default";
            String tableName = "sample_table";
            String partitionPath = "partitionField=value";
            client.appendPartition(dbName, tableName, partitionPath);
        } catch (TException e) {
            e.printStackTrace();
        }
    }
}
