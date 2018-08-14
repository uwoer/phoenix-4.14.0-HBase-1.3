package org.apache.phoenix;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.phoenix.mapreduce.index.IndexTool;
import org.apache.phoenix.query.QueryServices;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

/**
 * Created by uwoer on 2018/7/26.
 */
public class TestSubmitJob {

    public static void main(String[] args) {
        //    select TENANT_ID TABLE_CAT, TABLE_SCHEM, DATA_TABLE_NAME TABLE_NAME, true NON_UNIQUE, null INDEX_QUALIFIER, TABLE_NAME INDEX_NAME, 3 TYPE, ORDINAL_POSITION, COLUMN_NAME, CASE WHEN COLUMN_FAMILY IS NOT NULL THEN null WHEN SORT_ORDER = 1 THEN 'D' ELSE 'A' END ASC_OR_DESC, null CARDINALITY, null PAGES, null FILTER_CONDITION, ExternalSqlTypeId(DATA_TYPE) AS DATA_TYPE, SqlTypeName(DATA_TYPE) AS TYPE_NAME, DATA_TYPE TYPE_ID, COLUMN_FAMILY, COLUMN_SIZE, ARRAY_SIZE from SYSTEM."CATALOG" where TABLE_SCHEM is null and DATA_TABLE_NAME = 'visits' and COLUMN_NAME is not null order by INDEX_NAME,ORDINAL_POSITION

        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.property.clientPort", "2181");
//        config.set("hbase.zookeeper.quorum", "172.31.5.30") //Change the EMR master IP addr. to localhost when you debug locally
        config.set("hbase.zookeeper.quorum", "master,slave1,slave2");
        config.set("hbase.client.retries.number", "60"); //重试次数  见org.apache.hadoop.hbase.client.ClientScanner
        config.set("hbase.client.scanner.timeout.period", (60000*10)+""); //超时时间
        config.set("hbase.rootdir", "hdfs://master:9000/hbase");

        IndexTool indexingTool = new IndexTool();
        Configuration conf = new Configuration(config);
        conf.set(QueryServices.TRANSACTIONS_ENABLED, Boolean.TRUE.toString());
        indexingTool.setConf(conf);
        String[] cmdArgs = getArgValues(false, false, "", "\"visits\"", "\"visits_id_index\"");
//        String[] cmdArgs = getArgValues(false, false, "", "visits", "VISITS_DEVICEID_IDX");
//        String[] cmdArgs = getArgValues(false, false, "", "MY_TABLE", "MY_INDEX4");
        List<String> cmdArgList = new ArrayList<>(Arrays.asList(cmdArgs));
        cmdArgList.addAll(Arrays.asList(new String[0]));
        try {
            indexingTool.run(cmdArgList.toArray(new String[cmdArgList.size()]));
        } catch (Exception e) {
            e.printStackTrace();
        }
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
