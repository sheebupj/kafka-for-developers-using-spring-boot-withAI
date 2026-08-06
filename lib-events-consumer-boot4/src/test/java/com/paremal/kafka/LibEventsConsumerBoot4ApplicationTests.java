package com.paremal.kafka;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

@Import(TestcontainersConfiguration.class)
@SpringBootTest(properties = "spring.flyway.enabled=false")
class LibEventsConsumerBoot4ApplicationTests {

	@Test
	void contextLoads() {
	}

}
