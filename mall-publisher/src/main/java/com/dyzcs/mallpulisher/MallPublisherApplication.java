package com.dyzcs.mallpulisher;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan(basePackages = "com.dyzcs.mallpulisher.mapper")
public class MallPublisherApplication {

	public static void main(String[] args) {
		SpringApplication.run(MallPublisherApplication.class, args);
	}

}
