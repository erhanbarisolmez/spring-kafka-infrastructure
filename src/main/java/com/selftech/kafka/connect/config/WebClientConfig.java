package com.selftech.kafka.connect.config;

import java.time.Duration;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;

import io.netty.channel.ChannelOption;
import reactor.netty.http.client.HttpClient;

/**
 * WebClient Configuration for Kafka Connect REST API
 *
 * Creates a dedicated WebClient bean for Kafka Connect REST API calls.
 * Uses reactive programming with Project Reactor for non-blocking I/O.
 *
 * Key Features:
 * - Connection timeout: 5s (configurable)
 * - Read timeout: 30s (configurable)
 * - Max in-memory size: 10MB
 * - Default Content-Type: application/json
 *
 * @see com.selftech.kafka.connect.client.KafkaConnectClient
 */
@Configuration
public class WebClientConfig {

    /**
     * Create WebClient bean for Kafka Connect REST API
     *
     * @param properties Kafka Connect configuration properties
     * @return Configured WebClient instance
     */
    @Bean
    @Qualifier("kafkaConnectWebClient")
    public WebClient kafkaConnectWebClient(KafkaConnectProperties properties) {
        // Create HttpClient with custom timeouts
        HttpClient httpClient = HttpClient.create()
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, properties.getConnectionTimeout())
            .responseTimeout(Duration.ofMillis(properties.getReadTimeout()));

        return WebClient.builder()
            .baseUrl(properties.getUrl())
            .defaultHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .clientConnector(new ReactorClientHttpConnector(httpClient))
            .codecs(configurer -> configurer.defaultCodecs()
                .maxInMemorySize(10 * 1024 * 1024)) // 10MB
            .build();
    }
}
