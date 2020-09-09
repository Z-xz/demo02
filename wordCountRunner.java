package demo02;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class wordCountRunner {
    public static class wordCountMapper extends Mapper<LongWritable,Text,Text,LongWritable> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //1.将Text类型的value  转换成 string
            String datas = value.toString();

            //2.将这一行用 " " 切分出各个单词
            String[] words = datas.split(" ");

            //3.遍历数组,输出<单词,1>【一个单词输出一次】
            for (String word : words) {

                //输出数据
                //context   上下文对象
                context.write(new Text(word),new LongWritable(1));
            }
        }
    }
    public static class wordCountReducer extends Reducer<Text, LongWritable,Text,LongWritable> {
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

            /**
             * key 表示的是单词
             * values 表示的是好多个1
             */

            //定义一个计数,用于求和
            int sum = 0;

            //遍历这一组  kv  的所有v ,累加到count中
            for (LongWritable value : values) {

                //.get可以将LongWritable类型转换成Integer
                sum += value.get();

            }
            //输出结果
            context.write(key, new LongWritable(sum));
        }
    }
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        //创建一个Configuration实体类对象
        Configuration conf = new Configuration();
        Job wcjob = Job.getInstance(conf);
        // 指定我这个job所在的jar包
        // wcjob.setJar("/home/hadoop/wordcount.jar");
        wcjob.setJarByClass(wordCountRunner.class);

        wcjob.setMapperClass(wordCountMapper.class);
        wcjob.setReducerClass(wordCountReducer.class);

        //设置我们的业务逻辑Mapper 类的输出 key 和 value  的数据类型
        wcjob.setMapOutputKeyClass(Text.class);
        wcjob.setMapOutputValueClass(LongWritable.class);

        //设置我们的业务逻辑 Reducer 类的输入key 和 value 的数据类型
        wcjob.setMapOutputKeyClass(Text.class);
        wcjob.setOutputValueClass(LongWritable.class);

        long startTime=System.currentTimeMillis();   //获取开始时间


        //指定要处理的数据所在的位置
        FileInputFormat.setInputPaths(wcjob,"D:\\wordcount.txt");
        //指定处理完成之后的结果所保存的位置
        FileOutputFormat.setOutputPath(wcjob, new Path("D:\\result"));

        // 向yarn集群提交这个job
        boolean res = wcjob.waitForCompletion(true);
        long endTime=System.currentTimeMillis(); //获取结束时间
        System.out.println(res?0:1);
        System.out.println("程序运行时间： "+(endTime-startTime)+"ms");
    }
}
