package index.models;

public class CSVLineObject {
    public static final String DELIMITER = ",";

    private final String url;
    private final String title;
    private final String content;
    private final String publishedDateString;
    private final int domainRank;

    public static CSVLineObject parseCSVLine(String csvLine) {
        return new CSVLineObject(csvLine);
    }

    private CSVLineObject(String csvLineString) {
        String[] csvSplit = csvLineString.split(DELIMITER);

        if(csvSplit.length != 5) {
            System.out.println("Incorrect value for [" + csvLineString + "]");
        }

        this.url = csvSplit[0];
        this.title = csvSplit[1];
        this.content = csvSplit[2];
        this.publishedDateString = csvSplit[3];
        this.domainRank = Integer.parseInt(csvSplit[4]);
    }

    public String getUrl() {
        return url;
    }

    public String getTitle() {
        return title;
    }

    public String getContent() {
        return content;
    }

    public String getPublishedDateString() {
        return publishedDateString;
    }

    public int getDomainRank() {
        return domainRank;
    }
}
