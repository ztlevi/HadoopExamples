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

public class CommonFriendsStepTwo {
    static class CommonFriendsStepTwoMapper extends Mapper<LongWritable, Text, Text, Text>{
        //A I,K,C,B,G,F,H,O,D,       B A,F,J,E,

        private Text k=new Text();
        private Text v=new Text();
        @Override
        protected void map(LongWritable key, Text value,Context context)throws IOException, InterruptedException {
            String line = value.toString();
            String[] split = line.split("\t");
            String[] persons = split[1].split(",");

            // 对用户进行排序，以免出现GF、FG被视为不同的组合
            Arrays.sort(persons);
            v.set(split[0]);

            // 对整个persons数组做两两组合拼接
            for(int i=0;i<persons.length-1;i++){
                for(int j=i+1;j<persons.length;j++){
                    // 输出 <I-J A>
                    k.set(persons[i] + "-" + persons[j]);
                    context.write(k, v);
                }
            }
        }
    }

    static class CommonFriendsStepTwoReducer extends Reducer<Text, Text, Text, Text>{
        // <A-E,B> <A-E,C>.....
        private Text v = new Text();

        @Override
        protected void reduce(Text pair, Iterable<Text> friends,Context context)throws IOException, InterruptedException {
            StringBuilder sb=new StringBuilder();
            for(Text f : friends){
                sb.append(f).append(" ");
            }
            // <A-E, B C ..>
            v.set(sb.toString());
            context.write(pair, v);
        }
    }
    public static void main(String[] args) throws Exception, IOException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(CommonFriendsStepTwo.class);

        job.setMapperClass(CommonFriendsStepTwoMapper.class);
        job.setReducerClass(CommonFriendsStepTwoReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path("input_common"));
        FileOutputFormat.setOutputPath(job, new Path("output_common"));

        boolean res = job.waitForCompletion(true);

        System.exit(res ? 0 : 1);
    }
}

