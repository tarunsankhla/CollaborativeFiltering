/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Project/Maven2/JavaApp/src/main/java/${packagePath}/${mainClassName}.java to edit this template
 */

package edu.neu.csye6220.collaborativefiltering;

/**
 *
 * @author tarun
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CollaborativeFiltering {

    public static class UserMovieMapper extends Mapper<Object, Text, Text, Text> {
        private Text userMoviePair = new Text();
        private Text rating = new Text();

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            System.out.println("Mapper setup started");
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            long startTime = System.currentTimeMillis();

            String[] parts = value.toString().split(",");
            if (parts.length == 4) { // Adjust for four fields: userId, movieId, rating, timestamp
                String userId = parts[0];
                String movieId = parts[1];
                String userRating = parts[2];

                // Emit user-movie pair and rating
                userMoviePair.set(userId + "," + movieId);
                rating.set(userRating);
                context.write(userMoviePair, rating);
                System.out.println("Processed user-movie pair: " + userMoviePair.toString());
            } else {
                System.err.println("Invalid input line: " + value.toString());
            }

            long endTime = System.currentTimeMillis();
            System.out.println("Mapper execution time for record: " + (endTime - startTime) + " ms");
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            System.out.println("Mapper cleanup completed");
        }
    }

    public static class CosineSimilarityReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            System.out.println("Reducer setup started");
        }

       @Override
public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    long startTime = System.currentTimeMillis();

    System.out.println("Reducer processing key: " + key.toString());
    Map<String, Float> userRatings = new HashMap<>();

    try {
        for (Text value : values) {
            String[] userRating = value.toString().split(",");
            if (userRating.length == 2) { // Ensure the value is in the correct format
                userRatings.put(userRating[0], Float.parseFloat(userRating[1]));
            } else {
                System.err.println("Invalid value format for key: " + key.toString() + ", value: " + value.toString());
            }
        }
    } catch (Exception e) {
        System.err.println("Error parsing values for key: " + key.toString() + ", values: " + values);
        e.printStackTrace();
        return;
    }

    // Calculate cosine similarity
    try {
        for (Map.Entry<String, Float> entry1 : userRatings.entrySet()) {
            for (Map.Entry<String, Float> entry2 : userRatings.entrySet()) {
                if (!entry1.getKey().equals(entry2.getKey())) {
                    float dotProduct = entry1.getValue() * entry2.getValue();
                    float magnitude1 = (float) Math.sqrt(entry1.getValue() * entry1.getValue());
                    float magnitude2 = (float) Math.sqrt(entry2.getValue() * entry2.getValue());
                    float similarity = dotProduct / (magnitude1 * magnitude2);

                    context.write(new Text(entry1.getKey() + "," + entry2.getKey()), new Text("Similarity: " + similarity));
                    System.out.println("Computed similarity for " + entry1.getKey() + " and " + entry2.getKey() + ": " + similarity);
                }
            }
        }
    } catch (Exception e) {
        System.err.println("Error calculating similarity for key: " + key.toString());
        e.printStackTrace();
    }

    long endTime = System.currentTimeMillis();
    System.out.println("Reducer execution time for key " + key.toString() + ": " + (endTime - startTime) + " ms");
}

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            System.out.println("Reducer cleanup completed");
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: CollaborativeFiltering <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Collaborative Filtering");
        job.setJarByClass(CollaborativeFiltering.class);

        job.setMapperClass(UserMovieMapper.class);
        job.setReducerClass(CosineSimilarityReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        long jobStartTime = System.currentTimeMillis();
        boolean jobComplete = job.waitForCompletion(true);
        long jobEndTime = System.currentTimeMillis();

        if (jobComplete) {
            System.out.println("Job completed successfully");
        } else {
            System.err.println("Job failed");
        }

        System.out.println("Total job execution time: " + (jobEndTime - jobStartTime) + " ms");
        System.exit(jobComplete ? 0 : 1);
    }
}