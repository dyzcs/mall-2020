package com.dyzcs.mallchart;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;

@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class})
public class MallChartApplication {

	public static void main(String[] args) {
		SpringApplication.run(MallChartApplication.class, args);
	}

}
