package com.paremal.kafka.config;

import io.swagger.v3.oas.models.ExternalDocumentation;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
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
                                .email("support@example.com"))
                        .license(new License()
                                .name("Apache 2.0")
                                .url("https://www.apache.org/licenses/LICENSE-2.0")))
                .externalDocs(new ExternalDocumentation()
                        .description("Kafka for Developers — Spring Boot")
                        .url("https://github.com/sheebupj/kafka-for-developers-using-spring-boot-withAI"));
    }
}
