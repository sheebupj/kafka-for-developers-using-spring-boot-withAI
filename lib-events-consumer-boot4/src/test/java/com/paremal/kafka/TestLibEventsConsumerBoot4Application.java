package com.paremal.kafka;

import org.springframework.boot.SpringApplication;

public class TestLibEventsConsumerBoot4Application {

	public static void main(String[] args) {
		SpringApplication.from(LibEventsConsumerBoot4Application::main).with(TestcontainersConfiguration.class).run(args);
	}

}
