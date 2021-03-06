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
import java.util.ArrayList;
import java.util.HashSet;


/**
 * Created by keno on 5/11/15.
 */
public class Job1 extends Mapper<LongWritable, Text, Text, Mutation> {
    static HashSet<String> teams = new HashSet<String>();
    static final Value count = new Value("1".getBytes());
    static final Text hashTagFamily = new Text("hashtag");
    static final Text wordFamily = new Text("word");
    public static final Text nameFamily = new Text("Team Name");

    @Override
    public void map(LongWritable key, Text value, Context context) {
        Path path = ((FileSplit) context.getInputSplit()).getPath();
        Text teamMeta = new Text(path.getName().substring(0, path.getName().lastIndexOf('.')));
        String[] teamData = path.getName().split("##");
        String teamHashTag = teamData[1];
        String conference = path.getParent().getName();
        String[] words = value.toString().split("\\s+");
        if (!teams.contains(teamHashTag)){
            // Dummy write to ensure that each team has at least one "win" and "lose" entry
            teams.add(teamHashTag);
            write(teamMeta, "win", conference, context, count);
            write(teamMeta, "lose", conference, context, count);
        }
        for (String word : words) {
            if (word.equalsIgnoreCase("win") || word.equalsIgnoreCase("lose")) {
                write(teamMeta, word, conference, context, count);
            }
        }
    }

    private void write(Text teamMeta, String word, String conference, Context context, Value count){
        Text wordText = new Text(word.toLowerCase());
        long timestamp = System.currentTimeMillis();
        ColumnVisibility colVis = new ColumnVisibility(conference + "&" + word);
        Mutation mutation = new Mutation(teamMeta);
        mutation.put(wordFamily, wordText, colVis, timestamp, count);
        try {
            context.write(null, mutation);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
