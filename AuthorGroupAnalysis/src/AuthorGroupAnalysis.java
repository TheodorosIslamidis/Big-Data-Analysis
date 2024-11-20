import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.lang.module.Configuration;

import javax.naming.Context;
import javax.tools.Tool;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.w3c.dom.Text;

public class AuthorGroupAnalysisTool extends Configured implements Tool {

    public static class GroupSizeMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static Text dummyKey = new Text("groupSize");
        private IntWritable groupSize = new IntWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(";");
            if (fields.length > 1) {
                String[] authors = fields[1].split("\\|");
                groupSize.set(authors.length);
                context.write(dummyKey, groupSize);
            }
        }
    }

    public static class GroupSizeReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            int count = 0;

            for (IntWritable val : values) {
                sum += val.get();
                count++;
            }

            context.write(new Text("TotalGroupSize"), new IntWritable(sum));
            context.write(new Text("TotalGroupCount"), new IntWritable(count));
        }
    }

    public static class BelowAverageMapper extends Mapper<Object, Text, Text, IntWritable> {
        private double averageGroupSize;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            averageGroupSize = Double.parseDouble(context.getConfiguration().get("averageGroupSize"));
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(";");
            if (fields.length > 1) {
                String[] authors = fields[1].split("\\|");
                int size = authors.length;
                if (size < averageGroupSize) {
                    context.write(new Text("BelowAverageCount"), new IntWritable(1));
                }
            }
        }
    }

    public static class BelowAverageReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable val : values) {
                count += val.get();
            }
            context.write(new Text("GroupsBelowAverage"), new IntWritable(count));
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        // First Job - Calculate total group size and count
        Job job1 = Job.getInstance(conf, "Calculate Group Size and Count");
        job1.setJarByClass(AuthorGroupAnalysisTool.class);
        job1.setMapperClass(GroupSizeMapper.class);
        job1.setReducerClass(GroupSizeReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job1, new Path(args[0])); // Local input path
        Path tempOutput = new Path("temp_output");
        FileOutputFormat.setOutputPath(job1, tempOutput);

        if (!job1.waitForCompletion(true)) {
            System.err.println("Error with Job 1!");
            return 1;
        }

        // Read the output of Job 1 from local filesystem to calculate average group size
        BufferedReader reader = new BufferedReader(new FileReader("temp_output/part-r-00000"));
        String line;
        int totalGroupSize = 0;
        int totalGroupCount = 0;

        while ((line = reader.readLine()) != null) {
            String[] parts = line.split("\t");
            if (parts[0].equals("TotalGroupSize")) {
                totalGroupSize = Integer.parseInt(parts[1]);
            } else if (parts[0].equals("TotalGroupCount")) {
                totalGroupCount = Integer.parseInt(parts[1]);
            }
        }

        reader.close();

        double averageGroupSize = (double) totalGroupSize / totalGroupCount;
        conf.set("averageGroupSize", String.valueOf(averageGroupSize));

        // Second Job - Filter groups below the average size
        Job job2 = Job.getInstance(conf, "Count Groups Below Average Size");
        job2.setJarByClass(AuthorGroupAnalysisTool.class);
        job2.setMapperClass(BelowAverageMapper.class);
        job2.setReducerClass(BelowAverageReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job2, new Path(args[0])); // Same local input path
        FileOutputFormat.setOutputPath(job2, new Path(args[1])); // Local output path for final results

        return job2.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new AuthorGroupAnalysisTool(), args);
        System.exit(res);
    }
}
