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
public class Job1 extends Mapper<LongWritable, Text, Text, Mutation> {
    @Override
    public void map(LongWritable key, Text value, Context context) {
        String[] words = value.toString().split("\\s+");
        for (String word : words) {
            if (word.equalsIgnoreCase("win") || word.equalsIgnoreCase("lose")) {
                Path path = ((FileSplit) context.getInputSplit()).getPath();
                String teamData = path.getName();
                String teamName = teamData.split("##")[0];
                String teamHashTag = teamData.split("##")[1];
                String conference = path.getParent().getName();
                Text teamID = new Text(teamName);
                Text hashTagText = new Text(teamHashTag);
                Text wordText = new Text(word.toLowerCase());
                Value count = new Value("1".getBytes());
                long timestamp = System.currentTimeMillis();
                ColumnVisibility colVis = new ColumnVisibility(conference);
                Mutation mutation = new Mutation(teamID);
                mutation.put(hashTagText, wordText, colVis, timestamp, count);

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
