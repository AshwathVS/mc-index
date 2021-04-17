package index;

import index.models.CSVLineObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.WritableComparable;
import utils.DateUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class InvertedIndexWritable implements WritableComparable {
    private static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
    private static final String CUSTOM_WRITABLE_DELIMITER = "@@@";
    private static final String PIPE = "|";

    private String url;
    private int domainRank;
    private String publishedDate;
    private List<Integer> wordIndexes;

    public InvertedIndexWritable(String url, int domainRank, String publishedDate, List<Integer> wordIndexes) {
        this.url = url;
        this.domainRank = domainRank;
        this.publishedDate = publishedDate;
        this.wordIndexes = wordIndexes;
    }

    public InvertedIndexWritable() {
    }

    public static InvertedIndexWritable generateWriteableFromCSVObject(CSVLineObject csvLineObject, List<Integer> wordIndexes, String urlHash) {
        return new InvertedIndexWritable(urlHash, csvLineObject.getDomainRank(), csvLineObject.getPublishedDateString(), wordIndexes);
    }

    @Override
    public String toString() {
        return url + CUSTOM_WRITABLE_DELIMITER +
                domainRank + CUSTOM_WRITABLE_DELIMITER +
                publishedDate + CUSTOM_WRITABLE_DELIMITER +
                StringUtils.join(wordIndexes, "|") +
                PIPE;
    }

    private static Date parseDate(String date) {
        return DateUtils.parseDate(date, DATE_FORMAT);
    }

    @Override
    public int compareTo(Object o) {
        InvertedIndexWritable iiw = (InvertedIndexWritable) o;

        // first priority for domain rank
        if(this.domainRank != iiw.domainRank) {
            return Integer.compare(this.domainRank, iiw.domainRank);
        }

        if(this.publishedDate != iiw.publishedDate) {
            return parseDate(this.publishedDate).compareTo(parseDate(iiw.publishedDate));
        }

        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.url + CUSTOM_WRITABLE_DELIMITER +
                this.publishedDate + CUSTOM_WRITABLE_DELIMITER +
                this.domainRank + CUSTOM_WRITABLE_DELIMITER +
                StringUtils.join(wordIndexes, ","));
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        String inp = dataInput.readUTF();
        String[] inpArray = inp.split(CUSTOM_WRITABLE_DELIMITER);
        this.url = inpArray[0];
        this.publishedDate = inpArray[1];
        this.domainRank = Integer.parseInt(inpArray[2]);
        this.wordIndexes = utils.StringUtils.convertIntegerListString(inpArray[3]);
    }
}
