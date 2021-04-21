package index;

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
import utils.FileUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;


/**
 * Copy of org.apache.hadoop.mapreduce.lib.output.TextOutputFormat, needed this to override LineRecordWriter.
 * Before writing to the file, byte calculation per every line is done to make reads fast.
 */
public class CustomTextOutputFormat extends FileOutputFormat<Text, Text> {
    public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext job) throws IOException {
        Configuration conf = job.getConfiguration();
        boolean isCompressed = getCompressOutput(job);
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
            return new LineRecordWriter(fileOut, keyValueSeparator, conf.get(InvertedIndexJob.OUTPUT_PATH_CONF_KEY));
        } else {
            return new LineRecordWriter(new DataOutputStream(codec.createOutputStream(fileOut)), keyValueSeparator, conf.get(InvertedIndexJob.OUTPUT_PATH_CONF_KEY));
        }
    }

    protected static class LineRecordWriter extends RecordWriter<Text, Text> {
        private static final String utf8 = "UTF-8";
        private static final String WORD_TO_BYTE_DATA_FILE_PATH = "WORD_TO_BYTE_DATA";
        private static final byte[] newline;
        protected DataOutputStream out;
        private final byte[] keyValueSeparator;
        private final String outputPath;
        private final int keyValueSeparatorByteCount;
        private long totalBytesWritten = 0;
        private final Map<String, Pair<Long, Integer>> wordToByteOffsetAndLimitMapping = new HashMap<>();

        public LineRecordWriter(DataOutputStream out, String keyValueSeparator, String outputPath) {
            this.out = out;

            try {
                this.keyValueSeparator = keyValueSeparator.getBytes("UTF-8");
                this.outputPath = outputPath;
                this.keyValueSeparatorByteCount = this.keyValueSeparator.length;
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
            this.out.close();
        }

        static {
            try {
                newline = "\n".getBytes("UTF-8");
            } catch (UnsupportedEncodingException var1) {
                throw new IllegalArgumentException("can't find UTF-8 encoding");
            }
        }
    }
}
