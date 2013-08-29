package hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCount{          //word count using mapreduce package
    public static class Map extends Mapper<LongWritable,Text,Text,IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word=new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line= value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while(tokenizer.hasMoreTokens()){
                word.set(tokenizer.nextToken());
                context.write(word, one);
            }
        }  //map


    }   //class hadoop.experimental_finished.wc3.Mapper
    public static class Red extends Reducer<Text,IntWritable,Text,IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int sum=0;
            while(values.iterator().hasNext())
                sum+=values.iterator().next().get();
            context.write(key,new IntWritable(sum));
        }
    }    //class hadoop.experimental_finished.wc3.Reducer

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Job job=new Job(new Configuration(),"chaowei_experiment");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(WordCount.Map.class);
        job.setCombinerClass(WordCount.Red.class);
        job.setReducerClass(WordCount.Red.class);     //???


        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);   //necessary. otherwise defaulted to LongWritable
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        job.waitForCompletion(true);
        System.out.println("------------------------------Job completed---------------------------------------");

    } // main
}
