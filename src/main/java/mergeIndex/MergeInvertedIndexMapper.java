package mergeIndex;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MergeInvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if(value != null) {
            String[] keyValue = value.toString().split("\t");
            context.write(new Text(keyValue[0]), new Text(keyValue[1]));
        }
    }
}
