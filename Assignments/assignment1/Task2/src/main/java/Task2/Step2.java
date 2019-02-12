package Task2;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//import recommendpkg.Step1.Step1_ToItemPreMapper;
//import recommendpkg.Step1.Step1_ToUserVectorReducer;

public class Step2 {
    public static class Step2_UserVectorToCooccurrenceMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text k = new Text();
        private IntWritable v = new IntWritable(1);

        @Override
        public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
            String[] tokens = Recommend.DELIMITER.split(values.toString());
            for (String token: tokens) {
                for (String eachtoken: tokens) {
                    k.set(token.substring(0, token.indexOf(":")) + ":" + eachtoken.substring(0, eachtoken.indexOf(":")));
                    context.write(k, v);
                }
            }
        }
    }

    public static class Step2_UserVectorToConoccurrenceReducer extends Reducer<Text, IntWritable, Text, Text> {
        private Text v = new Text();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val: values) {
                sum += val.get();
            }
            v.set(Integer.toString(sum));
            context.write(key, v);
        }
    }

    public static void run(Map<String, String> path) throws IOException, ClassNotFoundException, InterruptedException {
        //get configuration info
        Configuration conf = Recommend.config();
        //get I/O path
        Path input = new Path(path.get("Step2Input"));
        Path output = new Path(path.get("Step2Output"));
        //delete last saved output
        HDFSAPI hdfs = new HDFSAPI(new Path(Recommend.HDFS));
        hdfs.delFile(output);
        //set job
        Job job = Job.getInstance(conf, "Step2");
        job.setJarByClass(Step2.class);

        job.setMapperClass(Step2_UserVectorToCooccurrenceMapper.class);
        job.setCombinerClass(Step2_UserVectorToConoccurrenceReducer.class);
        job.setReducerClass(Step2_UserVectorToConoccurrenceReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        //run job
        job.waitForCompletion(true);
    }
}
