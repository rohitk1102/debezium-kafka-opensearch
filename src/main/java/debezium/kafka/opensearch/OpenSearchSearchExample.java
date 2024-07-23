package debezium.kafka.opensearch;

import org.apache.http.HttpHost;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.io.IOException;

public class OpenSearchSearchExample {
    private RestHighLevelClient client;

    public OpenSearchSearchExample() {
        this.client = new RestHighLevelClient(RestClient.builder(
                new HttpHost("localhost", 9200, "http")));
    }

    public void search() {
        SearchRequest searchRequest = new SearchRequest("konnect");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery()); // Replace with your query
        searchRequest.source(searchSourceBuilder);

        try {
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            System.out.println("Search Results: " + searchResponse);
        } catch (IOException e) {
            throw new RuntimeException("Error in search "+ e.getMessage());
        }
    }

    public void fuzzySearch(String fieldName, String searchValue) {
        SearchRequest searchRequest = new SearchRequest("konnect");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery(fieldName, searchValue).fuzziness("AUTO")); // Fuzzy search query
        searchRequest.source(searchSourceBuilder);

        try {
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            System.out.println("Search Results: " + searchResponse);
        } catch (IOException e) {
            throw new RuntimeException("Error in search "+ e.getMessage());
        }
    }

    public void close() throws IOException {
        client.close();
    }

    public static void main(String[] args) {
        OpenSearchSearchExample example = new OpenSearchSearchExample();
        example.search();
        String key ="id";
        String value = "45a897b0-e766-4c6c-896b-4e5c45523ef2";
        System.out.println("Fuzzy search -------");
        example.fuzzySearch(key, value);
        try {
            example.close();
        } catch (IOException e) {
            throw new RuntimeException("Error in search "+ e.getMessage());
        }
    }
}
