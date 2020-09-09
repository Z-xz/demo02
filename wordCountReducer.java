package demo02;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class wordCountReducer extends Reducer<Text, LongWritable,Text,LongWritable> {
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

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
