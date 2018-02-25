package edu.bu.cs755;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Task3 {
    //these will be used for the keys in the map produced by this result
    private final static IntWritable one = new IntWritable(1);
    private final static IntWritable two = new IntWritable(2);

    public static class GetTripPrice extends Mapper<Object, Text, Text, MapWritable>{
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            // set the input string
            String line = value.toString();
            // split the string, on commas, into a list of strings
            String[] fields = line.split(",");
            // only process records with exactly 17 fields, the seconds and the fare are greater than 0, thus discarding some malformed records
            if  (fields.length == 17 && Double.parseDouble(fields[16]) > 0 && Double.parseDouble(fields[4]) > 0) {

                // Assign the medallion, fare and minutes for this ride to writable types
                Text medallion = new Text(fields[0]);
                DoubleWritable minutes = new DoubleWritable(Double.parseDouble(fields[4])/60);
                DoubleWritable fare = new DoubleWritable(Double.parseDouble(fields[16]));

                // Set up the map which will be output by this map
                MapWritable moneyMinutes = new MapWritable();

                // Assign the fare and minutes to the map with the keys 1 and 2
                moneyMinutes.put(one, minutes);
                moneyMinutes.put(two,fare);

                // write to the context the medallion number and the money, minutes map
                context.write(medallion, moneyMinutes);
            }
        }
    }

    public static class SumFaresPerMinute extends Reducer<Text,MapWritable,Text,DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();
        public void reduce(Text key, Iterable<MapWritable> values,
                           Context context
        ) throws IOException, InterruptedException {

            // use these to keep a running count of total minutes and fare for this cab
            double minutes = 0;
            double fare = 0;

            for (MapWritable val : values) {
                // add the minutes for this ride to the minutes total
                minutes += ((DoubleWritable) val.get(one)).get();
                // add the fare for this ride to the fare total
                fare += ((DoubleWritable) val.get(two)).get();
            }

            // troubleshooting
            /*
            System.out.println("---" + key + "---\n");
            System.out.println(minutes + "\n");
            System.out.println(fare + "\n");
            System.out.println(fare/minutes + "\n");
            System.out.println("--- END END END --- \n");
            */

            // Set the result to the sum of all the fares divided by the sum of all of the minutes
            result.set(fare/minutes);
            //write to the context the medallion number and money per minute for this cab
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job =  new Job(conf, "task3");
        job.setJarByClass(Task3.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritable.class);
        job.setMapperClass(GetTripPrice.class);
        //job.setCombinerClass(SumFaresPerMinute.class);
        job.setReducerClass(SumFaresPerMinute.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}