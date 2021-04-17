package index;

import index.models.CSVLineObject;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import utils.RedisUtils;
import utils.StringUtils;

import java.io.IOException;
import java.util.*;

public class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, InvertedIndexWritable> {

    private static final String CSV_HEADERS = "url,title,content,published_date,domain_rank";

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String content = value.toString();

        if(!CSV_HEADERS.equals(content) && isValidContent(content)) {
            CSVLineObject csvLineObject = CSVLineObject.parseCSVLine(content);
            String urlHash = StringUtils.getSHA256Hash(csvLineObject.getUrl());

            RedisUtils.set(urlHash, csvLineObject.getUrl());

            String allWords = csvLineObject.getTitle() + " " + csvLineObject.getContent();

            StringTokenizer stringTokenizer = new StringTokenizer(allWords);
            HashMap<String, List<Integer>> wordVsOccurrenceIndexList = new HashMap<>();

            int wordIndex = 1;
            while(stringTokenizer.hasMoreTokens()) {
                String token = stringTokenizer.nextToken().toLowerCase();

                if(!wordVsOccurrenceIndexList.containsKey(token)) {
                    wordVsOccurrenceIndexList.put(token, new ArrayList<>());
                }

                wordVsOccurrenceIndexList.get(token).add(wordIndex);
                wordIndex++;
            }

            Text text = new Text();
            for(Map.Entry<String, List<Integer>> wordVsOccurrence : wordVsOccurrenceIndexList.entrySet()) {
                if(!StopWords.isStopWord(wordVsOccurrence.getKey())) {
                    text.set(wordVsOccurrence.getKey());
                    context.write(text, InvertedIndexWritable.generateWriteableFromCSVObject(csvLineObject, wordVsOccurrence.getValue(), urlHash));
                }
            }
        }
    }

    private boolean isValidContent(String content) {
        return content.split(CSVLineObject.DELIMITER).length == 5;
    }
}
