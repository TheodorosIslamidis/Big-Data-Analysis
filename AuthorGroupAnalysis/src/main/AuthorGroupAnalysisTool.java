import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AuthorGroupAnalysisTool extends Configured implements Tool {

    // Mapper
    public static class GroupSizeMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static Text groupKey = new Text("groupSize");  
        private IntWritable groupSize = new IntWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(";");
            if (fields.length > 1) {
                String[] authors = fields[1].split("\\|");
                groupSize.set(authors.length);
                context.write(groupKey, groupSize);  
            }
        }
    }

    // Reducer
    public static class GroupSizeReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        	int totalGroupSize = 0;
            int totalGroupCount = 0;
            int belowAverageCount = 0;
            List<Integer> groupSizes = new ArrayList<>();//JRE must be compatible with 1.7
            
            // First Iteration
            for (IntWritable val : values) {
            	int size=val.get();
                totalGroupSize += size;
                totalGroupCount++;
                groupSizes.add(size);
            }

            // Calculate Average
            double averageGroupSize = (double) totalGroupSize / totalGroupCount;
            

            // Second Iteration
            for (int size: groupSizes) {
                if (size < averageGroupSize) {
                    belowAverageCount++;
                }
            }
            
            context.write(new Text("TotalGroupSize"), new IntWritable(totalGroupSize));
            context.write(new Text("TotalGroupCount"), new IntWritable(totalGroupCount));
            context.write(new Text("BelowAverageCount"), new IntWritable(belowAverageCount));
        }
    }

    // Run
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "Author Group Analysis");

        job.setJarByClass(AuthorGroupAnalysisTool.class);
        job.setMapperClass(GroupSizeMapper.class);
        job.setReducerClass(GroupSizeReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setNumReduceTasks(2);  // You can also try with 2 reducers

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
    	
        long startTime = System.currentTimeMillis();

        // Tool Execution
        int res = ToolRunner.run(new AuthorGroupAnalysisTool(), args);

        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;

        // Print time in ms
        System.out.println("Runtime in milliseconds: " + duration);

        System.exit(res);
    }
}
