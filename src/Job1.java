package edu.cse.buffalo.cse587;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;



/**
 * Created by keno on 5/11/15.
 */
public class Job1 {
    public class Map extends Mapper<LongWritable, Text, Text, Mutation> {
        @Override
        public void map(LongWritable key, Text value, Context context) {
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
