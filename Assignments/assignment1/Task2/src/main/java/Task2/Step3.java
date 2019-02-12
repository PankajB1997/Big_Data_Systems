package Task2;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Step3 {
    public static class Step31_UserVectorSplitterMapper extends Mapper<IntWritable, Text, Text, FloatWritable> {
        private final static Text k = new Text();
        private final static FloatWritable v = new FloatWritable();

        @Override
        public void map(IntWritable key, Text values, Context context)
                throws IOException, InterruptedException {
            String[] tokens = Recommend.DELIMITER.split(values.toString());
            String[] itemAndScore;
            for (String token: tokens) {
                itemAndScore = token.split(":");
                k.set(itemAndScore[0] + ":" + key.get());
                v.set(Float.parseFloat(itemAndScore[1]));
                context.write(k, v); // Write k-v of the form <"userID:itemID", score>
            }
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
        Job job =Job.getInstance(conf,"Step3_1");
        job.setJarByClass(Step3.class);

        job.setMapperClass(Step31_UserVectorSplitterMapper.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job,input);
        FileOutputFormat.setOutputPath(job,output);
        //run job
        job.waitForCompletion(true);
    }

    public static class Step32_CooccurrenceColumnWrapperMapper extends Mapper<Text, IntWritable, Text, IntWritable> {
//        private final static Text k = new Text();
//        private final static IntWritable v = new IntWritable();

        @Override
        public void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {

            context.write(key, value);
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

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        // run job
        job.waitForCompletion(true);
    }
}
