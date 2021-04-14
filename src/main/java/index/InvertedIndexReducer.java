package index;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class InvertedIndexReducer extends Reducer<Text, InvertedIndexWritable, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<InvertedIndexWritable> values, Context context) throws IOException, InterruptedException {
        StringBuilder stringBuilder = new StringBuilder();

        for(InvertedIndexWritable iiw : values) {
            stringBuilder.append(iiw.toString());
            stringBuilder.append(',');
        }

        context.write(key, new Text(stringBuilder.substring(0, stringBuilder.length() - 1)));
    }
}
