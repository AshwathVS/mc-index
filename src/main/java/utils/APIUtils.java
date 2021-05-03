package utils;


import com.google.gson.Gson;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import index.models.ClearCacheRequestBody;

import java.util.ArrayList;

public class APIUtils {
    private static final String CACHE_CLEAR_API = "http://localhost:8080/clear-cache";

    public static void executeCacheClearAPI(ClearCacheRequestBody clearCacheRequestBody) {
        try {
            HttpResponse<String> response = Unirest.post(CACHE_CLEAR_API).header("Content-Type", "application/json").body(new JsonNode(new Gson().toJson(clearCacheRequestBody))).asString();
            System.out.println(response.getBody());
        } catch (UnirestException ex) {
            ex.printStackTrace();
        }
    }

    public static void main(String[] args) {
        APIUtils.executeCacheClearAPI(new ClearCacheRequestBody(new ArrayList<>(){{add("news");}}));
    }
}
