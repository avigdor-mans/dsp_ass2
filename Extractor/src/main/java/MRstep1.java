import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MRstep1 {

    public static class MapperClass extends Mapper<LongWritable, Text, NGram, LongWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

//            splitted[0] - The actual n-gram
//            splitted[1] - The year for this aggregation
//            splitted[2] - The number of times this n-gram appeared in this year
//            splitted[3] - The number of pages this n-gram appeared on in this year
//            splitted[4] - The number of books this n-gram appeared in during this year

            String[] splitted = value.toString().split(" "); // splited by " " just for our corpus but shuled be splited by "\t"
            String[] words = splitted[0].split("/"); // splited by "/" just for our corpus but shuled be splited by "\t"
            NGram newNGram = new NGram(new Text(words[0]),
                    new Text(words[1]),
                    new Text(words[2]));
            String newValue = splitted[2]+splitted[1].substring(0,1);// occurrences + corpus part
            context.write(newNGram, new LongWritable(Integer.parseInt(newValue)));
        }
    }

    public static class PartitionerClass extends Partitioner<NGram, LongWritable> {
        public int getPartition(NGram nGram, LongWritable longWritable, int i) {
            return nGram.hashCode() % i;
        }
    }

    public static class CombinerClass extends Reducer<NGram,LongWritable,NGram,LongWritable> {

        @Override
        protected void reduce(NGram key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum1 = 0;
            long sum2 = 0;
            for(LongWritable value: values){
                long part = value.get()%10;
                long occurrences = value.get()/10;
                if(part==1){
                    sum1 +=occurrences;
                }
                else {
                    sum2 +=occurrences;
                }
            }
            long N = sum1+sum2;

            key.setFirstHalfCounter(new LongWritable((sum1)));
            key.setSecondHalfCounter(new LongWritable((sum2)));



            context.getCounter("COUNTER_N",key.toString()).increment(N); // N
            context.getCounter("COUNTER_1_1_"+ sum1,key.toString()).increment(1); // Nr0
            context.getCounter("COUNTER_1_2_"+ sum1,key.toString()).increment(sum2); // Tr01
            context.getCounter("COUNTER_2_1_"+ sum2,key.toString()).increment(1); // Nr1
            context.getCounter("COUNTER_2_2_"+ sum1,key.toString()).increment(sum1); //Tr10

            context.write(key, new LongWritable(N));
        }
    }

    public static class ReducerClass extends Reducer<NGram, LongWritable, NGram, LongWritable> {

        @Override
        protected void reduce(NGram key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

        long firs = key.getFirstHalfCounter().get();

        }
    }
}
