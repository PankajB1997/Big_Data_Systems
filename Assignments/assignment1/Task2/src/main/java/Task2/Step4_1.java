package Task2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Step4_1 {
    public static class Step4_PartialMultiplyMapper extends Mapper<LongWritable, Text, Text, Text> {
        private String filename;
        private Text k = new Text();
        private Text v = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSplit split = (FileSplit) context.getInputSplit();
            filename = split.getPath().getParent().getName(); //file name of the data set
        }

        @Override
        public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
            String[] key_value = Recommend.TAB_DELIMITER.split(values.toString());
            System.out.println(key_value[0]);
            System.out.println(key_value[1]);
            String[] tokens = Recommend.DELIMITER.split(key_value[1]);
            String[] row, row2;
            // input from user splitter mapper in step 3_1
            if (filename.equals("step3_1")) {
                System.out.println("Inside 3_1");
                for (String token: tokens) {
                    for (String token2: tokens) {
                        row = token.split(":");
                        row2 = token2.split(":");
                        System.out.println(row[0] + "," + row2[0]);
                        k.set(row[0] + "," + row2[0]);
                        System.out.println(row2[1] + "," + key_value[0]);
                        v.set(row2[1] + "," + key_value[0]);
                        context.write(k, v);
                    }
                }
            }
            // input from Co-occurrence matrix in step 3_2
            else if (filename.equals("step3_2")) {
                System.out.println("Inside 3_2");
                for (String token: tokens) {
                    row = token.split(":");
                    System.out.println(key_value[0].toString() + "," + row[0]);
                    k.set(key_value[0].toString() + "," + row[0]);
                    System.out.println(row[1]);
                    v.set(row[1]);
                    context.write(k, v);
                }
            }
        }
    }

    public static class Step4_AggregateReducer extends Reducer<Text, Text, Text, Text> {
        private Text k = new Text();
        private Text v = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            System.out.println("Key: " + key.toString());
            int countUsers = 0;
            int cooccurrence_count = -1;
            ArrayList<String> users = new ArrayList<String>();
            for (Text value: values) {
                if (value.toString().indexOf(",") != -1) {
                    users.add(value.toString());
                    countUsers++;
                }
                else {
                    cooccurrence_count = Integer.parseInt(value.toString());
                }
            }
            System.out.println("Cooccurrence count: " + cooccurrence_count);
            System.out.println("Count users: " + countUsers);
            if (cooccurrence_count != -1 && countUsers >= 1) {
                for (String user: users) {
                    String[] tokens = Recommend.DELIMITER.split(user);
                    float score = Float.parseFloat(tokens[0]);
                    String product = Float.toString(cooccurrence_count * score);
                    System.out.println("Printed Key: " + tokens[1] + "," + key.toString());
                    k.set(tokens[1] + "," + key.toString());
                    System.out.println("Printed Value: " + product);
                    v.set(product);
                    context.write(k, v);
                }
            }
        }
    }

    public static void run(Map<String, String> path) throws IOException, InterruptedException, ClassNotFoundException {
        //get configuration info
        Configuration conf = Recommend.config();
        // get I/O path
        Path input1 = new Path(path.get("Step4_1Input1"));
        Path input2 = new Path(path.get("Step4_1Input2"));
        Path output = new Path(path.get("Step4_1Output"));
        // delete last saved output
        HDFSAPI hdfs = new HDFSAPI(new Path(Recommend.HDFS));
        hdfs.delFile(output);
        // set job
        Job job = Job.getInstance(conf);
        job.setJarByClass(Step4_1.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Step4_1.Step4_PartialMultiplyMapper.class);
        job.setReducerClass(Step4_1.Step4_AggregateReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, input1, input2);
        FileOutputFormat.setOutputPath(job, output);

        job.waitForCompletion(true);
    }
}