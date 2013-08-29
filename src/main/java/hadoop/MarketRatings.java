package hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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

/**
 * Created with IntelliJ IDEA.
 * User: cwei
 * Date: 7/30/13
 * Time: 11:44 PM
 * To change this template use File | Settings | File Templates.
 */
public class MarketRatings {
    public static class AppMapper extends Mapper<LongWritable,Text,Text,Text>{
        private Text loc=new Text();
        private Text rating=new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] row=value.toString().split(",");

            if(row.length>31){
                String city= row[4];
                String state=row[6];

                int count=0;
                for(int c=11;c<=31;c++)
                    if(row[c].equals("Y"))count++;
                count=(count*100)/21; //gets 1-100 rating of the market

                int rated=0;
                if(count>0)rated=1;

                loc.set(city+", "+state);
                rating.set(1+"\t"+rated+"\t"+count);

                context.write(loc, rating);


            }
        } //map

    }

    public static class AppReducer extends Reducer<Text,Text,Text,Text>{

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int rating = 0;
            int numRated = 0;
            int numTotal = 0;

            while(values.iterator().hasNext()){
                String tokens[] = (values.iterator().next().toString()).split("\t");
                int tot = Integer.parseInt(tokens[0]);
                int num = Integer.parseInt(tokens[1]); //gets number of markets
                int val = Integer.parseInt(tokens[2]); //gets rating

                if(val > 0) //filters out markets with no data
                {
                    rating = (rating*numRated + val*num)/(numRated+num);
                    numRated = numRated + num;
                }
                numTotal = numTotal + tot;
            }

            if(rating>0)
                context.write(key, new Text(numTotal + "\t" + numRated + "\t" + rating));

        }
    } //reducer

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job=new Job(new Configuration(),"market rating using mapreduce");
        job.setJarByClass(MarketRatings.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapperClass(AppMapper.class);
        job.setReducerClass(AppReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        job.waitForCompletion(true);



        /*          deprecated version configuration

        JobConf conf = new JobConf(hadoop.experimental_finished.hadoop.experimental_finished.MarketRatings.class);
        conf.setJobName("hadoop.experimental_finished.hadoop.experimental_finished.MarketRatings");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(MapClass.class);
        conf.setReducerClass(Reduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
         */

    }

}
