import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class Extractor {
    private static String BUCKET_NAME = "bucketqoghawn0ehuw2njlvyexsmxt5dczxfwc";


    public static class MapperClass extends Mapper<LongWritable, Text, NGram, LongWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            System.err.println("Text 'value' is:" + value.toString());
            System.out.println("Text 'value' is:" + value.toString());
            String[] splitted = value.toString().split(" ");
            if(splitted.length < 3){
                System.err.println("splitted.length < 3");
                System.out.println("splitted.length < 3");
            }

            System.err.println("splitted[0] " + splitted[0]);
            System.err.println("splitted[0] " + splitted[1]);
            System.err.println("splitted[0] " + splitted[2]);
            System.out.println("splitted[0] " + splitted[0]);
            System.out.println("splitted[0] " + splitted[1]);
            System.out.println("splitted[0] " + splitted[2]);

            context.write(new NGram(new Text(splitted[0]),
                                    new Text(splitted[1]),
                                    new Text(splitted[2]),
                                    new LongWritable(1)), new LongWritable(1));
        }
    }

    public static class PartitionerClass extends Partitioner<NGram, LongWritable>{
        public int getPartition(NGram nGram, LongWritable longWritable, int i) {
            return nGram.hashCode() % i;
        }
    }

    public static class ReducerClass extends Reducer<NGram, LongWritable, NGram, LongWritable>{
        @Override
        protected void reduce(NGram key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;

            for(LongWritable value: values){
                sum += value.get();
            }

            context.write(key, new LongWritable(sum));

        }
    }

    public static void main(String[] args) throws Exception {
        System.err.println("Input file path " + args[1]);
        System.err.println("Output file path" + args[2]);

        Configuration conf = new Configuration();
        conf.set("BUCKET_NAME", BUCKET_NAME);
        conf.set("language", "heb");
        Job job = Job.getInstance(conf, "XXX");
        job.setJarByClass(Extractor.class);
//        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(NGram.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(NGram.class);
        job.setOutputValueClass(LongWritable.class);
//        job.setInputFormatClass(TextInputFormat.class);
//        FileInputFormat.addInputPath(job, new Path(args[1]));
        MultipleInputs.addInputPath(job,new Path(args[1]), TextInputFormat.class, MapperClass.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.setOutputFormatClass(TextOutputFormat.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
