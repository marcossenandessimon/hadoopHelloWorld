import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by msimon on 05/07/17.
 */
public class MapClass extends Mapper<LongWritable, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        String line = value.toString();
        StringTokenizer stringTokenizer = new StringTokenizer(line, " ");

        while( stringTokenizer.hasMoreTokens()){
            word.set(stringTokenizer.nextToken());
            context.write(word, one);
        }
    }

}
