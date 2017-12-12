import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CommonFriendsStepOne {
    public static class CommonFriendsStepOneMapper extends Mapper<LongWritable, Text, Text, Text>{
        private Text k = new Text();
        private Text v = new Text();

        // A:B,C,D,F,E,O       B--> A ; C-->A ; D-->A ; F-->A ....
        @Override
        protected void map(LongWritable key, Text value,Context context)throws IOException, InterruptedException {
            String line = value.toString();
            String[] split = line.split(":");
            String[] friends = split[1].split(",");

            v.set(split[0]);
            for(String f : friends){
                k.set(f);
                context.write(k, v);
            }
        }
    }

    public static class CommonFriendsStepOneReducer extends Reducer<Text, Text, Text, Text>{
        /*
         * 输入的数据 <B A> <B E> <B F> <B J>
         * 输出的结果 <B A,E,F,J>
         */
        private Text v=new Text();
        @Override
        protected void reduce(Text friend, Iterable<Text> persons,Context context)throws IOException, InterruptedException {
            StringBuilder sb=new StringBuilder();
            for(Text p : persons){
                sb.append(p).append(",");
            }
            v.set(sb.toString());
            context.write(friend, v);
        }
    }

    public static void main(String[] args) throws Exception, IOException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(CommonFriendsStepOne.class);

        job.setMapperClass(CommonFriendsStepOneMapper.class);
        job.setReducerClass(CommonFriendsStepOneReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path("input_common"));
        FileOutputFormat.setOutputPath(job, new Path("output_common"));

        boolean res = job.waitForCompletion(true);

        System.exit(res ? 0 : 1);
    }
}