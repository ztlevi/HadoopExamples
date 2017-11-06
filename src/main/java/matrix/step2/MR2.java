package matrix.step2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class MR2 {
    private static String inPath = "./input_matrix/matrix1.txt";
    private static String outPath = "./output_matrix_step2/";
    // 将step1的转置路径作为全局缓存
    private static String cache = "./output_matrix_step1/";

    public int run() {
        try {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "Matrix multiply Step2");

            // add distrubited cached file
            job.addCacheArchive(new URI(cache + "#matrix2"));

            job.setJarByClass(MR2.class);

            // set job Mapper class and Reducer class
            job.setMapperClass(Mapper2.class);
            job.setCombinerClass(Reducer2.class);
            job.setReducerClass(Reducer2.class);

            // set Mapper output type
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            // set Reducer output type
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            Path inputPath = new Path(inPath);
            Path outputPath = new Path(outPath);

            FileInputFormat.addInputPath(job, inputPath);

            FileOutputFormat.setOutputPath(job, outputPath);

            return job.waitForCompletion(true) ? 1 : -1;
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        return -1;
    }

    public static void main(String[] args) {
        int result = 0;
        result = new MR2().run();
        if (result == 1) {
            System.out.println("Step 2 run successfully...");
        } else if (result == -1) {
            System.out.println("Step2 failed...");
        }
    }
}

