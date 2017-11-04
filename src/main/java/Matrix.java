import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Matrix {
    public static class Mapper1 extends Mapper<LongWritable, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outValue = new Text();

        /**
         * key:1
         * value: 1 1_0,2_3,3_-1,4_2,5_-3
         */
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] rowAndLine = value.toString().split("\t");

            // matrix row num
            String row = rowAndLine[0];
            String[] lines = rowAndLine[1].split(",");

            // ["1_0","2_3","3_-1","4_2","5_-3"]
            for (int i = 0; i < lines.length; i++) {
                String column = lines[i].split("_")[0];
                String valueStr = lines[i].split("_")[1];
                //key: column num   value: rowNum_value
                outKey.set(column);
                outValue.set(row + "_" + valueStr);
                context.write(outKey, outValue);
            }
        }
    }

    public static class Reducer1 extends Reducer<Text, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outValue = new Text();

        //key: column   value:[rowNum_value,rowNum_value,rowNum_value...]
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            for (Text text : values) {
                // text: rowNum_value
                sb.append(text + ",");
            }
            String line = null;
            if (sb.toString().endsWith(",")) {
                line = sb.substring(0, sb.length() - 1);
            }

            outKey.set(key);
            outValue.set(line);
            context.write(outKey, outValue);
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
//        conf.set("fs.defaultFS", hdfs);
        Job job = Job.getInstance(conf, "Matrix multiply");
        job.setJarByClass(Matrix.class);

        // set job Mapper class and Reducer class
        job.setMapperClass(Matrix.Mapper1.class);
        job.setCombinerClass(Matrix.Reducer1.class);
        job.setReducerClass(Matrix.Reducer1.class);

        // set Mapper output type
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // set Reducer output type
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
