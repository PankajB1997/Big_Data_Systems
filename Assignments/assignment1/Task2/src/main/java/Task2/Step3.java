package Task2;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Step3 {
    public static class Step31_UserVectorSplitterMapper extends Mapper<IntWritable, Text, Text, Text> {
        private Text k = new Text();

        @Override
        public void map(IntWritable key, Text values, Context context)
                throws IOException, InterruptedException {
            k.set(key.toString());
            context.write(k, values);
        }
    }

    public static void run1(Map<String, String> path) throws IOException, ClassNotFoundException, InterruptedException {
        //get configuration info
        Configuration conf = Recommend.config();
        //get I/O path
        Path input = new Path(path.get("Step3Input1"));
        Path output = new Path(path.get("Step3Output1"));
        //delete the last saved output
        HDFSAPI hdfs = new HDFSAPI(new Path(Recommend.HDFS));
        hdfs.delFile(output);
        //set job
        Job job = Job.getInstance(conf,"Step3_1");
        job.setJarByClass(Step3.class);

        job.setMapperClass(Step31_UserVectorSplitterMapper.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job,input);
        FileOutputFormat.setOutputPath(job,output);
        //run job
        job.waitForCompletion(true);
    }

    public static class Step32_CooccurrenceColumnWrapperMapper extends Mapper<Text, Text, Text, Text> {
        private Text k = new Text();
        private Text v = new Text();

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = Recommend.DELIMITER.split(key.toString());
            k.set(tokens[0]);
            v.set(tokens[1] + ":" + value);
            context.write(k, v);
        }
    }

    public static class Step32_ConoccurrenceColumnWrapperReducer extends Reducer<Text, Text, Text, Text> {
        private Text v = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String combinedRow = "";
            for (Text value: values) {
                combinedRow += "," + value.toString();
            }
            v.set(combinedRow.substring(1));
            context.write(key, v);
        }
    }

    public static void run2(Map<String, String> path) throws IOException,
            ClassNotFoundException, InterruptedException {
        //get configuration info
        Configuration conf = Recommend.config();
        //get I/O path
        Path input = new Path(path.get("Step3Input2"));
        Path output = new Path(path.get("Step3Output2"));
        // delete the last saved output
        HDFSAPI hdfs = new HDFSAPI(new Path(Recommend.HDFS));
        hdfs.delFile(output);
        // set job
        Job job =Job.getInstance(conf, "Step3_2");
        job.setJarByClass(Step3.class);

        job.setMapperClass(Step32_CooccurrenceColumnWrapperMapper.class);
        job.setReducerClass(Step32_ConoccurrenceColumnWrapperReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        // run job
        job.waitForCompletion(true);
    }
}
