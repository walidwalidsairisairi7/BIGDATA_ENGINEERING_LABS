package lab2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;

public class WCMapper extends Mapper<Object, Text, Text, IntWritable> {
    public void map(Object key, Text value , Context context )
    throws java.io.IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split(" ");
        for (String word : words) {
            context.write(new Text(word), new IntWritable(1));
        }
    }
}   
