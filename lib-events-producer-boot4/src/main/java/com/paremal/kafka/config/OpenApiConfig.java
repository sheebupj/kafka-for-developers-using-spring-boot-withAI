package com.paremal.kafka.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI libraryEventsOpenApi() {
        return new OpenAPI()
                .info(new Info()
                        .title("Library Events Producer API")
                        .description("REST API to publish library events to Kafka")
                        .version("v1")
                        .contact(new Contact()
                                .name("Library Events Team")
                                .email("support@example.com")));
    }
}
