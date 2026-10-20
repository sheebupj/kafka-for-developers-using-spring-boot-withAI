package com.paremal.kafka.health;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Component("kafkaReadiness")
public class KafkaReadinessHealthIndicator implements HealthIndicator {

    private final Object bootstrapServers;

    public KafkaReadinessHealthIndicator(KafkaAdmin kafkaAdmin) {
        this.bootstrapServers = kafkaAdmin.getConfigurationProperties()
                .get(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG);
    }

    @Override
    public Health health() {
        Map<String, Object> config = new HashMap<>();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, (int) Duration.ofSeconds(2).toMillis());
        config.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, (int) Duration.ofSeconds(2).toMillis());
        config.put(AdminClientConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG,
                Duration.ofSeconds(1).toMillis());

        try (AdminClient adminClient = AdminClient.create(config)) {
            int brokerCount = adminClient.describeCluster().nodes().get(2, TimeUnit.SECONDS).size();
            if (brokerCount == 0) {
                return Health.down()
                        .withDetail("bootstrapServers", bootstrapServers)
                        .withDetail("reason", "No brokers available")
                        .build();
            }
            return Health.up()
                    .withDetail("bootstrapServers", bootstrapServers)
                    .withDetail("brokerCount", brokerCount)
                    .build();
        } catch (Exception ex) {
            return Health.down(ex)
                    .withDetail("bootstrapServers", bootstrapServers)
                    .build();
        }
    }
}
