package edu.cse.buffalo.cse587;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Map;

/**
 * Created by keno on 5/9/15.
 */
public class Main extends Configured implements Tool {

    @Override
    public int run(String[] strings) throws Exception {
        Job job1 = new Job(getConf(),Main.class.getName());
        String[] zookeepers = new String[2];
        String[] inputDir;
        String tableName = "";
        String finalTableName = "NBARankings";

        //this is bad, you know..
        zookeepers = new String[]{strings[0], strings[1]};
        inputDir = strings[2].split(",");
        tableName = strings[3];


        job1.setJarByClass(Main.class);
        job1.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job1, new Path(inputDir[0]), new Path(inputDir[1]));
        job1.setMapperClass(Job1.class);
        job1.setNumReduceTasks(0);
        job1.setOutputFormatClass(AccumuloOutputFormat.class);
        job1.setOutputKeyClass(Text.class);

        job1.setOutputValueClass(Mutation.class);

        //Table creation and initialization
        Instance instance = new ZooKeeperInstance(zookeepers[0], zookeepers[1]);
        Connector conn = instance.getConnector("root", "acc".getBytes());
        TableOperations tableOp = conn.tableOperations();
        SecurityOperations secOp = conn.securityOperations();
        if (tableOp.exists(tableName)){
            tableOp.delete(tableName);
        }
        tableOp.create(tableName);
        secOp.grantTablePermission("east", tableName, TablePermission.READ);
        secOp.grantTablePermission("west", tableName, TablePermission.READ);


        //Wordcount iterator
        IteratorSetting is = new IteratorSetting(10, "wCounter", SummingCombiner.class);
        SummingCombiner.setEncodingType(is, LongCombiner.Type.STRING);
        SummingCombiner.setLossyness(is, true);
        SummingCombiner.setCombineAllColumns(is, true);
        tableOp.attachIterator(tableName, is);

        AccumuloOutputFormat.setOutputInfo(job1.getConfiguration(), "root", "acc".getBytes(), true, tableName);


        AccumuloOutputFormat.setZooKeeperInstance(job1.getConfiguration(), zookeepers[0], zookeepers[1]);
        job1.waitForCompletion(true);

        //Rank the teams and write to a new table
        if (tableOp.exists(finalTableName)){
            tableOp.delete(finalTableName);
        }
        tableOp.create(finalTableName);
        secOp.grantTablePermission("east", finalTableName, TablePermission.READ);
        secOp.grantTablePermission("west", finalTableName, TablePermission.READ);
        secOp.grantTablePermission("root", finalTableName, TablePermission.READ);
        secOp.grantTablePermission("root", finalTableName, TablePermission.WRITE);


        createRankings(conn, finalTableName, tableName);
        return 0;
    }

    private void createRankings(Connector conn, String outputTableName, String inputTable) throws TableNotFoundException,
            MutationsRejectedException {
        /* Reads from the input table name, ranks teams, and writes to the output table
        * */
        Authorizations auths = new Authorizations("east", "west", "win", "lose");
        org.apache.accumulo.core.client.Scanner scanner = conn.createScanner(inputTable, auths);
        BatchWriter bw = conn.createBatchWriter(outputTableName, 10000000L, 120, 3);
        scanner.fetchColumnFamily(Job1.hashTagFamily);
        scanner.fetchColumnFamily(Job1.wordFamily);

        for (Map.Entry<Key, Value> kv : scanner){
            int value = Integer.parseInt(kv.getValue().toString());
            // Increase length argument for denser data sets
            String[] meta = kv.getKey().getRow().toString().split("##");
            String teamName = meta[0];
            ColumnVisibility cv = new ColumnVisibility(kv.getKey().getColumnVisibility());
            String teamHashTag = meta[1];
            Text key = new Text(getRowId(value, 6, teamHashTag));
            Value val = new Value(Integer.toString(value - 1).getBytes());
            Text wordText = new Text();
            kv.getKey().getColumnQualifier(wordText);
            Mutation m = new Mutation(key);
            // Creates the new table, with columns as specified
            m.put(Job1.nameFamily, new Text(teamName), cv,val);
            m.put(Job1.hashTagFamily, new Text(teamHashTag), cv, val);
            m.put(Job1.wordFamily, wordText, cv, val);
            bw.addMutation(m);
        }
        bw.close();
    }


    private static String getRowId(int value, int length, String suffix){
        int max = (int) Math.pow(10, length);
        return Math.abs(max - value) + "_" + suffix;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(CachedConfiguration.getInstance(), new Main(), args);
        System.exit(res);
    }

}
