package org.apache.phoenix;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.mapreduce.index.IndexTool;
import org.apache.phoenix.mapreduce.util.ConnectionUtil;
import org.apache.phoenix.query.QueryServices;

import java.sql.Connection;
import java.util.List;
import java.util.UUID;

/**
 * Created by uwoer on 2018/7/26.
 */
public class TestSql {

    public static void main(String[] args) {
        //    select TENANT_ID TABLE_CAT, TABLE_SCHEM, DATA_TABLE_NAME TABLE_NAME, true NON_UNIQUE, null INDEX_QUALIFIER, TABLE_NAME INDEX_NAME, 3 TYPE, ORDINAL_POSITION, COLUMN_NAME, CASE WHEN COLUMN_FAMILY IS NOT NULL THEN null WHEN SORT_ORDER = 1 THEN 'D' ELSE 'A' END ASC_OR_DESC, null CARDINALITY, null PAGES, null FILTER_CONDITION, ExternalSqlTypeId(DATA_TYPE) AS DATA_TYPE, SqlTypeName(DATA_TYPE) AS TYPE_NAME, DATA_TYPE TYPE_ID, COLUMN_FAMILY, COLUMN_SIZE, ARRAY_SIZE from SYSTEM."CATALOG" where TABLE_SCHEM is null and DATA_TABLE_NAME = 'visits' and COLUMN_NAME is not null order by INDEX_NAME,ORDINAL_POSITION

        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.property.clientPort", "2181");
//        config.set("hbase.zookeeper.quorum", "172.31.5.30") //Change the EMR master IP addr. to localhost when you debug locally
        config.set("hbase.zookeeper.quorum", "master,slave1,slave2");
        config.set("hbase.client.retries.number", "60"); //重试次数  见org.apache.hadoop.hbase.client.ClientScanner
        config.set("hbase.client.scanner.timeout.period", (60000*10)+""); //超时时间
        config.set("hbase.rootdir", "hdfs://master:9000/hbase");

//        Connection conn = DriverManager.getConnection(getUrl(), props);

        IndexTool indexingTool = new IndexTool();
        Configuration conf = new Configuration(config);
        conf.set(QueryServices.TRANSACTIONS_ENABLED, Boolean.TRUE.toString());
        final Configuration configuration = HBaseConfiguration.addHbaseResources(conf);
        Connection connection = null;
        try {
            connection = ConnectionUtil.getInputConnection(configuration);
//            connection.createStatement().execute("create index VISIT_ID_INDEX on \"visits\"(\"id\")");
//            connection.createStatement().execute("create index \"visits_id_index\" on \"visits\"(\"id\") INCLUDE(\"mobile\") ASYNC");
//            connection.createStatement().execute("create index \"visits_id_index\" on \"visits\"(\"id\") INCLUDE(\"mobile\")");
//            connection.createStatement().execute("explain select count(\"action_id\") as counts, substr(pk,0,10) as eventTime  from \"actions\" where  pk like '2018-03' and \"userId\" = 'FECAB64E-CBD1-4B06-934B-3367F74A43DF'  group by eventTime");
//            connection.createStatement().execute("create view  MY_TABLE2 (PK  varchar primary key, CF1.V1 varchar, CF1.V2 varchar, CF1.V3 varchar)");
//            connection.createStatement().execute("UPSERT INTO MY_TABLE1  VALUES('8','uwo8','8','8')");
            connection.createStatement().execute("create index VISITS_LOCUS_IDX on \"visits\"(\"userId\",\"eventTime\") INCLUDE(\"id\",\"login_userId\",\"resolution\",\"model\",\"countryName\",\"city\",\"osVersion\",\"appVersion\",\"platform\") ASYNC");
//            connection.createStatement().execute("UPSERT INTO MY_TABLE1(PK,V3)  VALUES('9','90')");
//            connection.createStatement().execute("UPSERT INTO MY_TABLE  VALUES('1','uwo1','1','10')");
//            connection.createStatement().execute("create index my_index5 on MY_TABLE(v3)");
//            connection.createStatement().execute("SELECT  * FROM \"visits\" limit 10");
//            byte[] qualifier = Bytes.toBytes("V3");
//            ImmutableBytesPtr qualifierPtr = new ImmutableBytesPtr(qualifier);
//            qualifierPtr.get();
            connection.commit();
        } catch (Exception e) {
            e.printStackTrace();
        }




//        Connection conn = DriverManager.getConnection(getUrl(), props);

    }

    public static String[] getArgValues(boolean directApi, boolean useSnapshot, String schemaName,
                                        String dataTable, String indxTable) {
        final List<String> args = Lists.newArrayList();
        if (schemaName != null) {
            args.add("-s");
            args.add(schemaName);
        }
        args.add("-dt");
        args.add(dataTable);
        args.add("-it");
        args.add(indxTable);
        if (directApi) {
            args.add("-direct");
            // Need to run this job in foreground for the test to be deterministic
            args.add("-runfg");
        }

        if (useSnapshot) {
            args.add("-snap");
        }

        args.add("-op");
        args.add("/tmp/" + UUID.randomUUID().toString());
        return args.toArray(new String[0]);
    }
}
