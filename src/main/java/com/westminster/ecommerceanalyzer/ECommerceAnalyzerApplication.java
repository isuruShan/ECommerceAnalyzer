package com.westminster.ecommerceanalyzer;

import org.apache.commons.dbcp.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.annotation.AsyncConfigurer;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.web.client.RestTemplate;

import javax.sql.DataSource;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Configuration
@SpringBootApplication
@EnableScheduling
public class ECommerceAnalyzerApplication {
    public static void main(String[] args) {
        SpringApplication.run(ECommerceAnalyzerApplication.class, args);
    }

    @Configuration
    public static class Config {
        public static final int  CORE_POOL_SIZE = 2;
        public static  final int MAX_POOL_SIZE = 2;
        public static  final int QUEUE_CAPACITY = 50;
        public static  final boolean ALLOW_CORE_THREAD_TIMEOUT = false;

        @Bean
        public RestTemplate restTemplate() {
            return new RestTemplate();
        }

        @Configuration
        @EnableAsync
        public class SpringAsyncConfig implements AsyncConfigurer {
            @Override
            public Executor getAsyncExecutor() {
                ThreadPoolTaskExecutor t = new ThreadPoolTaskExecutor();
                t.setCorePoolSize(CORE_POOL_SIZE);
                t.setMaxPoolSize(MAX_POOL_SIZE);
                t.setQueueCapacity(QUEUE_CAPACITY);
                t.setAllowCoreThreadTimeOut(ALLOW_CORE_THREAD_TIMEOUT);
                return t;
            }
        }

    }
}
