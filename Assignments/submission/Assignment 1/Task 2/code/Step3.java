package Task2;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// Further processing for matrices, to bring them into a form suitable for matrix multiplication

public class Step3 {
    public static class Step31_UserVectorSplitterMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text k = new Text();
        private Text v = new Text();

        @Override
        public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {

            // input is of the form < userID, “itemID:score,itemID:score,...” >
            // output to reducer is of the form < "itemID", "userID_user:score" >

            String[] key_value = Recommend.TAB_DELIMITER.split(values.toString());
            String[] tokens = Recommend.DELIMITER.split(key_value[1]);
            for (String token: tokens) {
                String[] vals = token.split(":");
                k.set(vals[0]);
                v.set(key_value[0] + "_user:" + vals[1]);
                context.write(k, v);
            }
        }
    }

    public static class Step31_UserVectorSplitterReducer extends Reducer<Text, Text, Text, Text> {
        private Text v = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            // input is of the form < "itemID", iterable("userID:score") >
            // output is of the form < "itemID", "userID_user:score,userID_user:score,..." >

            String scores_by_users = "";
            for (Text value: values) {
                scores_by_users += "," + value.toString();
            }
            scores_by_users = scores_by_users.replaceFirst(",", "");
            v.set(scores_by_users);
            context.write(key, v);
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
        job.setReducerClass(Step31_UserVectorSplitterReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job,input);
        FileOutputFormat.setOutputPath(job,output);
        //run job
        job.waitForCompletion(true);
    }

    public static class Step32_CooccurrenceColumnWrapperMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text k = new Text();
        private Text v = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // input is of the form < "itemA:itemB", cooccurrence_count >
            // output to reducer is of the form < "itemA", "itemB:cooccurrence_count" >

            String[] key_value = Recommend.TAB_DELIMITER.split(value.toString());
            String[] tokens = Recommend.DELIMITER.split(key_value[0]);
            k.set(tokens[0]);
            v.set(tokens[1] + ":" + key_value[1]);
            context.write(k, v);
        }
    }

    public static class Step32_ConoccurrenceColumnWrapperReducer extends Reducer<Text, Text, Text, Text> {
        private Text v = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            // input is of the form < "itemA", iterable("itemB:cooccurrence_count") >
            // output is of the form < "itemA", "itemB:cooccurrence_count,itemC:cooccurrence_count,..." >

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
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        // run job
        job.waitForCompletion(true);
    }
}
