package utils;

import org.apache.commons.math3.util.Pair;
import org.json.JSONObject;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class JsonToCSV {
    public static String escape(String text) {
        return text.replaceAll("[^a-zA-Z0-9 ]", " ");
    }

    public static void main(String[] args) throws Exception {
        List<Pair<String, String>> paths = new ArrayList<>() {{
            add(new Pair<>("/home/ashwath/Documents/MapReduce Project/archive/2018_01_112b52537b67659ad3609a234388c50a", "2018_01_112b52537b67659ad3609a234388c50a.csv"));
            add(new Pair<>("/home/ashwath/Documents/MapReduce Project/archive/2018_02_112b52537b67659ad3609a234388c50a", "2018_02_112b52537b67659ad3609a234388c50a.csv"));
            add(new Pair<>("/home/ashwath/Documents/MapReduce Project/archive/2018_03_112b52537b67659ad3609a234388c50a", "2018_03_112b52537b67659ad3609a234388c50a.csv"));
            add(new Pair<>("/home/ashwath/Documents/MapReduce Project/archive/2018_04_112b52537b67659ad3609a234388c50a", "2018_04_112b52537b67659ad3609a234388c50a.csv"));
            add(new Pair<>("/home/ashwath/Documents/MapReduce Project/archive/2018_05_112b52537b67659ad3609a234388c50a", "2018_05_112b52537b67659ad3609a234388c50a.csv"));
        }};

        String filepath = "/home/ashwath/Documents/MapReduce Project/archive/";

        for (Pair<String, String> path : paths) {
            // read folder
            String base_folder = path.getFirst();
            System.out.println("Parsing " + base_folder);

            // create CSV file
            String filename = path.getSecond();
            FileUtils.createFile(filepath, filename);

            // write title to CSV
            String[] columnHeaders = {"url", "title", "content", "published_date", "domain_rank"};
            FileUtils.writeLineToFile(filepath + filename, false, columnHeaders);

            // for each file in folder, write to CSV
            int count = 0;
            File[] fileList = new File(base_folder).listFiles();
            for (int i = 0; i < fileList.length; i++) {
                File file = fileList[i];
                System.out.println(i + 1 + "/" + fileList.length);
                if (!file.isDirectory()) {
                    String jsonString = Files.readString(Path.of(file.toURI()), StandardCharsets.UTF_8);
                    JSONObject jsonObject = new JSONObject(jsonString);
                    if ("english".equalsIgnoreCase(jsonObject.getString("language"))) {
                        count++;
                        String[] row = new String[columnHeaders.length];

                        row[0] = jsonObject.has("url") ? jsonObject.getString("url") : "";

                        row[1] = jsonObject.has("title") ? escape(jsonObject.getString("title")) : "";
                        row[2] = jsonObject.has("text") ? escape(jsonObject.getString("text")) : "";
                        row[3] = jsonObject.has("published") ? jsonObject.getString("published") : "";

                        int domainRank = -1;
                        if (jsonObject.has("thread") && jsonObject.getJSONObject("thread").has("domain_rank"))
                            domainRank = jsonObject.getJSONObject("thread").getInt("domain_rank");
                        row[4] = String.valueOf(domainRank);

                        FileUtils.writeLineToFile(filepath + filename, true, row);
                    }
                }
            }
            System.out.println("Total of " + count + " files have been converted in " + base_folder);
        }
    }
}
