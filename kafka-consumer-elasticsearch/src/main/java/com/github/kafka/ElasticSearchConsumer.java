package com.github.kafka;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author anil
 */
public class ElasticSearchConsumer {
    public static void main(String[] args) throws IOException {
       Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class);
        String jsonString = "{\"foo\":\"bar\"}";
        RestHighLevelClient client = createClient();
        IndexRequest request = new IndexRequest("twitter", "tweets").source(jsonString, XContentType.JSON);
        IndexResponse response = client.index(request, RequestOptions.DEFAULT);
        String id = request.id();
        logger.info(id);
        client.close();
    }

    public static RestHighLevelClient createClient() {
        //replace with your bonsai elastic search credentials
        String hostName = "kafka-twitter-3550458487.eu-central-1.bonsaisearch.net";
        String userName = "luoi2xte20";
        String password = "gihc18pno5";
        final BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(userName, password));

        RestClientBuilder builder = RestClient.builder(new HttpHost(hostName, 443, "https")).setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
            @Override
            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            }
        });
        return new RestHighLevelClient(builder);
    }
}
