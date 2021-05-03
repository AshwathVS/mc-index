package index.models;

import java.util.List;

public class ClearCacheRequestBody {
    private List<String> wordsToClearFromCache;

    public ClearCacheRequestBody(List<String> wordsToClearFromCache) {
        this.wordsToClearFromCache = wordsToClearFromCache;
    }

    public List<String> getWordsToClearFromCache() {
        return this.wordsToClearFromCache;
    }

    public void setWordsToClearFromCache(List<String> wordsToClearFromCache) {
        this.wordsToClearFromCache = wordsToClearFromCache;
    }
}
