package hadoop;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;          // mapred is deprecated. use mapreduce instead

/**
 * Created with IntelliJ IDEA.
 * User: cwei
 * Date: 7/29/13
 * Time: 10:48 PM
 * To change this template use File | Settings | File Templates.
 */
public class WordCount_deprecated {                   //word count use mapred package, deprecated
    public static class Map extends MapReduceBase implements Mapper<LongWritable,Text,Text,IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word=new Text();
        @Override
        public void map(LongWritable key, Text value, OutputCollector<Text,IntWritable> output, Reporter reporter) throws IOException {
            String line= value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while(tokenizer.hasMoreTokens()){
                word.set(tokenizer.nextToken());
                output.collect(word,one);
            }

        }    // map


    }  // class



    public static class Reduce extends MapReduceBase implements Reducer<Text,IntWritable,Text,IntWritable> {
        @Override
        public void reduce(Text key,Iterator<IntWritable> value, OutputCollector<Text,IntWritable> output, Reporter reporter) throws IOException {
            int sum=0;
            while(value.hasNext()){
                sum+=value.next().get();
            }
            output.collect(key,new IntWritable(sum));

        }    //reduce
    }   //class
    public static void main(String[] args) throws Exception {
        JobConf conf= new JobConf(WordCount_deprecated.class);
        conf.setJobName("word count hello world of hadoop in hadoop.experimental_finished.wc2");
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);
        conf.setMapperClass(WordCount_deprecated.Map.class);
        conf.setCombinerClass(WordCount_deprecated.Reduce.class);
        conf.setReducerClass(Reduce.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        JobClient.runJob(conf);

    }
}

