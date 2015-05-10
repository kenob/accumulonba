import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Created by keno on 5/9/15.
 */
public class Main extends Configured implements Tool {

    @Override
    public int run(String[] strings) throws Exception {
        Job job1 = new Job(getConf(),Main.class.getName());

        job1.setJarByClass(Main.class);
        job1.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job1, new Path(strings[2]));
        job1.setMapperClass(Job1.Map.class);
        job1.setNumReduceTasks(0);
        job1.setOutputFormatClass(AccumuloOutputFormat.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Mutation.class);
        AccumuloOutputFormat.setOutputInfo(job1.getConfiguration(), "root", "acc".getBytes(), true, strings[3]);
        AccumuloOutputFormat.setZooKeeperInstance(job1.getConfiguration(), strings[0], strings[1]);
        job1.waitForCompletion(true);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new Main(), args);
    }

    public class Job1{
        public class Map extends Mapper<LongWritable, Text, Text, Mutation> {
            @Override
            public void map(LongWritable key, Text value, Context context){
                String[] words = value.toString().split("\\s+");
                for (String word : words) {
                    if (word.equalsIgnoreCase("win") || word.equalsIgnoreCase("lose")) {
                        Path path = ((FileSplit) context.getInputSplit()).getPath();
                        String pathString = ((FileSplit) context.getInputSplit()).getPath().toString();
                        String[] pathNames = pathString.split("/");
                        String teamName = pathNames[pathNames.length - 1];
                        String conference = pathNames[pathNames.length - 2];
                        Text teamID = new Text(teamName);
                        Text wordText = new Text(word);
                        Text c = new Text(word);
                        Value count = new Value("1".getBytes());
                        long timestamp = System.currentTimeMillis();
                        ColumnVisibility colVis = new ColumnVisibility(conference);
                        Mutation mutation = new Mutation(wordText);
                        mutation.put(new Text(("team")), teamID, colVis, timestamp, count);

                        try {
                            context.write(null, mutation);
                        } catch (IOException e) {
                            e.printStackTrace();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }
    }
}
