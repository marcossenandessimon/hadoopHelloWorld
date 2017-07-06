import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by msimon on 05/07/17.
 */
public class WordCount extends Configured implements Tool{

    public static void main(String[] args) throws Exception{
        int exitCode = ToolRunner.run(new WordCount(), args);
        System.exit(exitCode);
    }

    public int run(String[] strings) throws Exception {
        if(strings.length != 2){
            System.err.printf(" Usage: " + getClass().getSimpleName() + " needs two arguments, input and output files");
            return -1;
        }
        Job job = new Job();
        job.setJarByClass(WordCount.class);
        job.setJobName("WordCounter");


        FileInputFormat.addInputPath(job, new Path(strings[0]));
        FileOutputFormat.setOutputPath(job, new Path(strings[1]));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(MapClass.class);
        job.setReducerClass(ReduceClass.class);

        int returnValue = job.waitForCompletion(true) ? 0 : 1 ;

        if(job.isSuccessful()){
            System.out.println("Job done!");
        }else if(!job.isSuccessful()){
            System.out.println("job not done!");
        }

        return returnValue;
    }
}
