package wordcountpkg;
//here is your own package name

// related
		import java.io.IOException;
		import java.util.Arrays;
		import java.util.StringTokenizer;
		 
		import org.apache.commons.logging.Log;
		import org.apache.commons.logging.LogFactory;
		import org.apache.hadoop.conf.Configuration;
		import org.apache.hadoop.fs.Path;
		import org.apache.hadoop.io.IntWritable;
		import org.apache.hadoop.io.Text;
		import org.apache.hadoop.mapred.JobConf;
		import org.apache.hadoop.mapreduce.Job;
		import org.apache.hadoop.mapreduce.Mapper;
		import org.apache.hadoop.mapreduce.Reducer;
		import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
		import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
		import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
		import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
		import org.apache.hadoop.util.GenericOptionsParser;
		
		
		
	public class WordCount {
		//Mapper 1
		    public static class Mapper1 extends Mapper<Object, Text, Text, IntWritable> {
		 
		        private Text word = new Text();
		        private final static IntWritable one = new IntWritable(1);
		 
		        public void map(Object key, Text value, Context context)
		                throws IOException, InterruptedException {
		            StringTokenizer itr = new StringTokenizer(value.toString());
		 
		            while (itr.hasMoreTokens()) {
		                word.set(itr.nextToken());
		                context.write(word, one);
		            }
		        }
		    }
		  //Mapper 2
		    public static class Mapper2 extends Mapper<Object, Text, Text, IntWritable> {
		 
		        private Text word = new Text();
		        private final static IntWritable one = new IntWritable(1);
		 
		        public void map(Object key, Text value, Context context)
		                throws IOException, InterruptedException {
		            StringTokenizer itr = new StringTokenizer(value.toString());
		 
		            while (itr.hasMoreTokens()) {
		                word.set(itr.nextToken());
		                context.write(word, one);
		            }
		        }
		    }
		 //sum the wordcount
		    public static class IntSumReducer extends
		            Reducer<Text, IntWritable, Text, IntWritable> {
		        private IntWritable result = new IntWritable();
		 
		        public void reduce(Text key, Iterable<IntWritable> values,
		                Context context) throws IOException, InterruptedException {
		 
		            int sum = 0;
		            for (IntWritable val : values) {
		                sum += val.get();
		            }
		            result.set(sum);
		            context.write(key, result);
		        }
		    }
		 ///////////////////////////////////////////////////////////////////////////////////
		 //main function
		    
		    public static void main(String[] args) throws Exception {
		        
		    	Configuration conf = new Configuration();
		        String[] otherArgs = new GenericOptionsParser(conf, args)
		                .getRemainingArgs();
		        //here you can change another way to get the input and output
		        
		        if (otherArgs.length != 3) {
		            System.err
		                    .println("Usage: wordcountmultipleinputs <input1> <input2> <out>");
		            System.exit(2);
		        }
		        
		        Job job = new Job(conf, "word count multiple inputs");
		        job.setJarByClass(WordCount.class);
		        
		        MultipleInputs.addInputPath(job, new Path(otherArgs[0]),
		                TextInputFormat.class, Mapper1.class);
		        MultipleInputs.addInputPath(job, new Path(otherArgs[1]),
		                TextInputFormat.class, Mapper2.class);
		 
		        job.setCombinerClass(IntSumReducer.class);
		        job.setReducerClass(IntSumReducer.class);
		        job.setMapOutputKeyClass(Text.class);
		        job.setMapOutputValueClass(IntWritable.class);
		        job.setOutputKeyClass(Text.class);
		        job.setOutputValueClass(IntWritable.class);
		        job.setNumReduceTasks(1);
		        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		        System.exit(job.waitForCompletion(true) ? 0 : 1);
		    }
		}
