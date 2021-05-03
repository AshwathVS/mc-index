package mergeIndex;

import index.CustomTextOutputFormat;
import index.InvertedIndexWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MergeInvertedIndexJob extends Configured implements Tool {
    public static final String OUTPUT_PATH_CONF_KEY = "OUTPUT_PATH";

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("Usage: <input path> <output path>");
            return -1;
        }

        Configuration conf = new Configuration();
        conf.setStrings(OUTPUT_PATH_CONF_KEY, args[1]);

        Job job = Job.getInstance(conf, "Merge Inverted Index");

        job.setJarByClass(MergeInvertedIndexJob.class);
        job.setMapperClass(MergeInvertedIndexMapper.class);
        job.setReducerClass(MergeInvertedIndexReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(CustomTextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MergeInvertedIndexJob(), args);
        System.exit(exitCode);
    }
}
