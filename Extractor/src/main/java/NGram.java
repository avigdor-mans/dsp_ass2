//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class NGram implements WritableComparable<NGram> {
    private Text w1, w2, w3;
    private LongWritable counter;

    public NGram() {
        this.w1 = new Text();
        this.w2 = new Text();
        this.w3 = new Text();
        this.counter = new LongWritable();
    }

    public NGram(Text w1, Text w2, Text w3, LongWritable counter) {
        this.w1 = w1;
        this.w2 = w2;
        this.w3 = w3;
        this.counter = counter;
    }

    public Text getW1() {
        return w1;
    }

    public Text getW2() {
        return w2;
    }

    public Text getW3() {
        return w3;
    }

    public LongWritable getCounter() {
        return counter;
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
            return this.counter.compareTo(o.getCounter());
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.w1.write(dataOutput);
        this.w2.write(dataOutput);
        this.w3.write(dataOutput);
        this.counter.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.w1.readFields(dataInput);
        this.w2.readFields(dataInput);
        this.w3.readFields(dataInput);
        this.counter.readFields(dataInput);
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
                "\t" + w3.toString() +
                "\t" + counter.toString() + "\n";
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

    public void setCounter(LongWritable counter) {
        this.counter = counter;
    }

    @Override
    public int hashCode() {
        return Objects.hash(getW1(), getW2(), getW3());
    }
}
