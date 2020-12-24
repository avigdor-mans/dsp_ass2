//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class NGram implements WritableComparable<NGram> {
    private Text w1, w2, w3;
    private LongWritable firstHalfCounter, secondHalfCounter, percent;

    public NGram() {
        this.w1 = new Text();
        this.w2 = new Text();
        this.w3 = new Text();
        this.firstHalfCounter = new LongWritable();
        this.secondHalfCounter = new LongWritable();
        this.percent = new LongWritable();
    }

    public NGram(Text w1, Text w2, Text w3) {
        this.w1 = w1;
        this.w2 = w2;
        this.w3 = w3;
        this.firstHalfCounter = new LongWritable();
        this.secondHalfCounter = new LongWritable();
        this.percent = new LongWritable();
    }

    public int compareTo(NGram o) {
        int cW1 = this.w1.compareTo(o.getW1());
        int cW2 = this.w2.compareTo(o.getW2());

        if(cW1 != 0){
            return cW1;
        }
        else if(cW2 != 0){
            return cW2;
        }
        else{
            return this.percent.compareTo(o.getPercent());
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.w1.write(dataOutput);
        this.w2.write(dataOutput);
        this.w3.write(dataOutput);
        this.firstHalfCounter.write(dataOutput);
        this.secondHalfCounter.write(dataOutput);
        this.percent.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.w1.readFields(dataInput);
        this.w2.readFields(dataInput);
        this.w3.readFields(dataInput);
        this.firstHalfCounter.readFields(dataInput);
        this.secondHalfCounter.readFields(dataInput);
        this.percent.readFields(dataInput);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NGram nGram = (NGram) o;
        return Objects.equals(getW1(), nGram.getW1()) &&
                Objects.equals(getW2(), nGram.getW2()) &&
                Objects.equals(getW3(), nGram.getW3());
    }

    @Override
    public String toString() {
        return
                w1.toString() +
                "\t" + w2.toString() +
                "\t" + w3.toString();
    }

    public void setW1(Text w1) {
        this.w1 = w1;
    }

    public void setW2(Text w2) {
        this.w2 = w2;
    }

    public void setW3(Text w3) {
        this.w3 = w3;
    }

    public void setFirstHalfCounter(LongWritable firstHalfCounter) { this.firstHalfCounter = firstHalfCounter; }

    public void setSecondHalfCounter(LongWritable secondHalfCounter) { this.secondHalfCounter = secondHalfCounter; }

    public void setPercent(LongWritable percent) { this.percent = percent; }

    public Text getW1() {
        return w1;
    }

    public Text getW2() {
        return w2;
    }

    public Text getW3() {
        return w3;
    }

    public LongWritable getFirstHalfCounter() { return firstHalfCounter; }

    public LongWritable getSecondHalfCounter() { return secondHalfCounter; }

    public LongWritable getPercent() { return percent; }


    @Override
    public int hashCode() {
        return Objects.hash(getW1(), getW2(), getW3());
    }
}
