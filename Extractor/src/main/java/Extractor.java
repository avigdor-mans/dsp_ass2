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
    private static String BUCKET_NAME = "bucketqoghawn0ehuw2njlvyexsmxt5dczxfwca";


    public static void main(String[] args) throws Exception {
        System.err.println("Input file path " + args[1]);
        System.err.println("Output file path" + args[2]);

        Configuration conf = new Configuration();
        conf.set("BUCKET_NAME", BUCKET_NAME);
        conf.set("language", "heb");
        Job job = Job.getInstance(conf, "XXX");
        job.setJarByClass(Extractor.class);
//        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(MRstep1.PartitionerClass.class);
        job.setReducerClass(MRstep1.ReducerClass.class);
        job.setMapOutputKeyClass(NGram.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(NGram.class);
        job.setOutputValueClass(LongWritable.class);
//        job.setInputFormatClass(TextInputFormat.class);
//        FileInputFormat.addInputPath(job, new Path(args[1]));
        MultipleInputs.addInputPath(job,new Path(args[1]), TextInputFormat.class, MRstep1.MapperClass.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.setOutputFormatClass(TextOutputFormat.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
