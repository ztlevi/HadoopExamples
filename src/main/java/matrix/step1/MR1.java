package matrix.step1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MR1 {
    private static String inPath = "input_matrix/";
    private static String outPaht = "output_matrix/";

    public static int run() {

        Configuration conf = new Configuration();
        Job job = null;
        try {
            job = Job.getInstance(conf, "Matrix multiply Step1");
        } catch (IOException e) {
            e.printStackTrace();
        }
        job.setJarByClass(MR1.class);

        // set job Mapper class and Reducer class
        job.setMapperClass(Mapper1.class);
        job.setCombinerClass(Reducer1.class);
        job.setReducerClass(Reducer1.class);

        // set Mapper output type
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // set Reducer output type
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        Path inputPath = new Path(inPath);
        Path outputPath = new Path(outPaht);
        try {
            FileInputFormat.addInputPath(job, inputPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
        FileOutputFormat.setOutputPath(job, outputPath);
        try {
            return job.waitForCompletion(true) ? 1 : -1;
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return -1;
    }

    public static void main(String[] args) {
        int result = -1;
        result = new MR1().run();
        if (result == 1) {
            System.out.println("step1 run successfully...");
        } else if (result == -1 ) {
            System.out.println("step1 failed...");
        }
    }
}
