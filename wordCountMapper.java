package demo02;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class wordCountMapper extends Mapper<LongWritable,Text,Text,LongWritable>{
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
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
