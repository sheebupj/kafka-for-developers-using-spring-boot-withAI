package com.paremal.kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.TimeZone;

@SpringBootApplication(scanBasePackages = {"com.paremal.kafka"})
public class LibEventsConsumerBoot4Application {

	public static void main(String[] args) {
		SpringApplication.run(LibEventsConsumerBoot4Application.class, args);
	}

}
