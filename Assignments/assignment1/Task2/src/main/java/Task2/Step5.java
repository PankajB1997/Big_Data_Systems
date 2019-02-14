package Task2;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Step5 {
    public static class Step5_FilterSortMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        // Student Name: Pankaj Bhootra.
        // My NUSNET ID is E0009011. The last 3 digits is thus the number 11.
        private static final int NUSNET_ID = 11;

        private IntWritable k = new IntWritable();
        private Text v = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] key_value = Recommend.TAB_DELIMITER.split(value.toString());
            String[] tokens = Recommend.DELIMITER.split(key_value[0]);
            if (Integer.parseInt(tokens[0]) == NUSNET_ID) {
                k.set(Integer.parseInt(tokens[0]));
                v.set(tokens[1] + "," + key_value[1]);
                context.write(k, v);
            }
        }
    }

    public static class Step5_FilterSortReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        private Text v = new Text();

        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            HashMap<String, Float> scores_by_item = new HashMap<String, Float>();
            for (Text value: values) {
                String[] item_score = Recommend.DELIMITER.split(value.toString());
                scores_by_item.put(item_score[0], Float.parseFloat(item_score[1]));
            }
            List<Map.Entry<String,Float>> sorted_list_items;
            sorted_list_items = SortHashMap.sortHashMap(scores_by_item);
            for(Map.Entry<String,Float> ilist : sorted_list_items) {
                v.set(ilist.getKey() + "\t" + ilist.getValue());
                context.write(key, v);
            }
        }
    }

    public static void run(Map<String, String> path) throws IOException, InterruptedException, ClassNotFoundException {
        //get configuration info
        Configuration conf = Recommend.config();
        // I/O path
        Path input = new Path(path.get("Step5Input"));
        Path output = new Path(path.get("Step5Output"));
        // delete last saved output
        HDFSAPI hdfs = new HDFSAPI(new Path(Recommend.HDFS));
        hdfs.delFile(output);
        // set job
        Job job = Job.getInstance(conf);
        job.setJarByClass(Step5.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Step5_FilterSortMapper.class);
        job.setReducerClass(Step5_FilterSortReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, output);

        job.waitForCompletion(true);
    }
}
