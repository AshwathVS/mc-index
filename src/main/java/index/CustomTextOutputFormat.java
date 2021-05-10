package index;

import index.models.ClearCacheRequestBody;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import utils.APIUtils;
import utils.FileUtils;
import utils.RedisUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Copy of org.apache.hadoop.mapreduce.lib.output.TextOutputFormat, needed this to override LineRecordWriter.
 * Before writing to the file, byte calculation per every line is done to make reads fast.
 */
public class CustomTextOutputFormat extends FileOutputFormat<Text, Text> {
    public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext job) throws IOException {
        Configuration conf = job.getConfiguration();
        boolean isCompressed = getCompressOutput(job);
        boolean cacheIndexOnRedis = conf.getBoolean(InvertedIndexJob.CACHE_IN_REDIS_CONF_KEY, false);

        String keyValueSeparator = conf.get("mapred.textoutputformat.separator", "\t");
        CompressionCodec codec = null;
        String extension = "";
        if (isCompressed) {
            Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(job, GzipCodec.class);
            codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
            extension = codec.getDefaultExtension();
        }

        Path file = this.getDefaultWorkFile(job, extension);
        FileSystem fs = file.getFileSystem(conf);
        FSDataOutputStream fileOut;
        fileOut = fs.create(file, false);
        if (!isCompressed) {
            return new LineRecordWriter(fileOut, keyValueSeparator, conf.get(InvertedIndexJob.OUTPUT_PATH_CONF_KEY), cacheIndexOnRedis);
        } else {
            return new LineRecordWriter(new DataOutputStream(codec.createOutputStream(fileOut)), keyValueSeparator, conf.get(InvertedIndexJob.OUTPUT_PATH_CONF_KEY), cacheIndexOnRedis);
        }
    }

    protected static class LineRecordWriter extends RecordWriter<Text, Text> {
        private static final String utf8 = "UTF-8";
        private static final String WORD_TO_BYTE_DATA_FILE_PATH = "WORD_TO_BYTE_MAP";
        private static final byte[] newline;
        protected DataOutputStream out;
        private final byte[] keyValueSeparator;
        private final String outputPath;
        private final int keyValueSeparatorByteCount;
        private final boolean cacheInRedis;
        private long totalBytesWritten = 0;
        private final Map<String, Pair<Long, Integer>> wordToByteOffsetAndLimitMapping = new HashMap<>();
        private final List<String> keysWritten;

        public LineRecordWriter(DataOutputStream out, String keyValueSeparator, String outputPath, boolean cacheInRedis) {
            this.out = out;
            this.cacheInRedis = cacheInRedis;
            try {
                this.keyValueSeparator = keyValueSeparator.getBytes("UTF-8");
                this.outputPath = outputPath;
                this.keyValueSeparatorByteCount = this.keyValueSeparator.length;
                this.keysWritten = new ArrayList<>();
            } catch (UnsupportedEncodingException var4) {
                throw new IllegalArgumentException("can't find UTF-8 encoding");
            }
        }

        private void writeObject(Object o) throws IOException {
            if (o instanceof Text) {
                Text to = (Text)o;
                this.out.write(to.getBytes(), 0, to.getLength());
            } else {
                this.out.write(o.toString().getBytes("UTF-8"));
            }

        }

        public synchronized void write(Text key, Text value) throws IOException {
            boolean nullKey = key == null;
            boolean nullValue = value == null;
            if (!nullKey || !nullValue) {
                int bytesWritten = 0;
                if (!nullKey) {
                    this.writeObject(key);
                    bytesWritten += key.getLength();
                }

                if (!nullKey && !nullValue) {
                    this.out.write(this.keyValueSeparator);
                    bytesWritten += this.keyValueSeparatorByteCount;

                    if(this.cacheInRedis) {
                        String cacheKey = InvertedIndexJob.CACHED_INDEX_PREFIX + key;
                        this.keysWritten.add(key.toString());
                        if(RedisUtils.containsKey(cacheKey)) {
                            RedisUtils.append(cacheKey, "," + value.toString());
                        } else {
//                            RedisUtils.set(cacheKey, value.toString());
                        }
                    }
                }

                if (!nullValue) {
                    this.writeObject(value);
                    bytesWritten += value.getLength();
                }

                this.out.write(newline);

                this.wordToByteOffsetAndLimitMapping.put(key.toString(), Pair.of(totalBytesWritten, bytesWritten));
                totalBytesWritten += (bytesWritten + newline.length);
            }
        }

        public synchronized void close(TaskAttemptContext context) throws IOException {
            FileUtils.writeToFile(this.outputPath + "/" + WORD_TO_BYTE_DATA_FILE_PATH, this.wordToByteOffsetAndLimitMapping);
            System.out.println("TOTAL BYTES WRITTEN: " + totalBytesWritten);

            if(this.cacheInRedis && !this.keysWritten.isEmpty()) {
                APIUtils.executeCacheClearAPI(new ClearCacheRequestBody(this.keysWritten));
            }

            this.out.close();
        }

        static {
            newline = "\n".getBytes(StandardCharsets.UTF_8);
        }
    }
}
