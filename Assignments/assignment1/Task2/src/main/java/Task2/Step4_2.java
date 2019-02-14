package Task2;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Step4_2 {
    public static class Step4_RecommendMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text k = new Text();
        private Text v = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] key_value = Recommend.TAB_DELIMITER.split(value.toString());
            String[] tokens = Recommend.DELIMITER.split(key_value[0]);
            k.set(tokens[0] + "," + tokens[1]);
            v.set(key_value[1]);
            context.write(k, v);
        }
    }

    public static class Step4_RecommendReducer extends Reducer<Text, Text, Text, Text> {
        private Text v = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            float sum = 0;
            for(Text value: values) {
                sum += Float.parseFloat(value.toString());
            }
            v.set(Float.toString(sum));
            context.write(key, v);
        }
    }

    public static void run(Map<String, String> path) throws IOException, InterruptedException, ClassNotFoundException {
        //get configuration info
        Configuration conf = Recommend.config();
        // get I/O path
        Path input = new Path(path.get("Step4_2Input"));
        Path output = new Path(path.get("Step4_2Output"));
        // delete last saved output
        HDFSAPI hdfs = new HDFSAPI(new Path(Recommend.HDFS));
        hdfs.delFile(output);
        // set job
        Job job =Job.getInstance(conf);
        job.setJarByClass(Step4_2.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Step4_2.Step4_RecommendMapper.class);
        job.setReducerClass(Step4_2.Step4_RecommendReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, output);

        job.waitForCompletion(true);
    }
}
