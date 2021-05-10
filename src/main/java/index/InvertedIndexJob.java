package index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class InvertedIndexJob extends Configured implements Tool {
    public static final String OUTPUT_PATH_CONF_KEY = "OUTPUT_PATH";
    public static final String CACHE_IN_REDIS_CONF_KEY = "CACHE_IN_REDIS";
    public static final String CACHED_INDEX_PREFIX = "CACHE_";

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Usage: <input path> <output path>");
            return -1;
        }

        Configuration conf = new Configuration();
        conf.setStrings(OUTPUT_PATH_CONF_KEY, args[1]);

        if(args.length == 3) {
            conf.setBoolean(CACHE_IN_REDIS_CONF_KEY, Boolean.parseBoolean(args[2]));
        }

        Job job = Job.getInstance(conf, "Inverted Index");

        job.setJarByClass(InvertedIndexJob.class);
        job.setMapperClass(InvertedIndexMapper.class);
        job.setReducerClass(InvertedIndexReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(InvertedIndexWritable.class);
        job.setOutputFormatClass(CustomTextOutputFormat.class);
        job.setNumReduceTasks(2);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
//        int exitCode = ToolRunner.run(new InvertedIndexJob(), args);
//        System.exit(exitCode);

        if (args.length < 2) {
            System.out.println("Usage: <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.setStrings(OUTPUT_PATH_CONF_KEY, args[1]);

        if(args.length == 3) {
            conf.setBoolean(CACHE_IN_REDIS_CONF_KEY, Boolean.parseBoolean(args[2]));
        }

        Job job = Job.getInstance(conf, "Inverted Index");

        job.setJarByClass(InvertedIndexJob.class);
        job.setMapperClass(InvertedIndexMapper.class);
        job.setReducerClass(InvertedIndexReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(InvertedIndexWritable.class);
        job.setOutputFormatClass(CustomTextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
