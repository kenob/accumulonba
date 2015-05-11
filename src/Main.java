package edu.cse.buffalo.cse587;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by keno on 5/9/15.
 */
public class Main extends Configured implements Tool {

    @Override
    public int run(String[] strings) throws Exception {
        Job job1 = new Job(getConf(),Main.class.getName());
        String[] zookeepers = new String[2];
        String inputDir = "";
        String tableName = "";

        //this is bad, you know..
        zookeepers = new String[]{strings[0], strings[1]};
        inputDir = strings[2];
        tableName = strings[3];


        job1.setJarByClass(Main.class);
        job1.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job1, new Path(inputDir));
        job1.setMapperClass(Job1.Map.class);
        job1.setNumReduceTasks(0);
        job1.setOutputFormatClass(AccumuloOutputFormat.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Mutation.class);
        AccumuloOutputFormat.setOutputInfo(job1.getConfiguration(), "root", "acc".getBytes(), true, tableName);
        AccumuloOutputFormat.setZooKeeperInstance(job1.getConfiguration(), zookeepers[0], zookeepers[1]);
        job1.waitForCompletion(true);
        return 0;
    }


    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(CachedConfiguration.getInstance(), new Main(), args);
        System.exit(res);
    }

}
