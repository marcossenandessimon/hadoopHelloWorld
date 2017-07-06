import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by msimon on 05/07/17.
 */
public class ReduceClass extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{

        int sum = 0;
        Iterator valuesIterator = values.iterator();

        while(valuesIterator.hasNext()){
            sum = sum + (Integer) valuesIterator.next();
        }

        context.write(key, new IntWritable(sum));

    }

}
