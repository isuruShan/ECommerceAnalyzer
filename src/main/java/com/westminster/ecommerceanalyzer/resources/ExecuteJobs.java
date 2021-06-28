package com.westminster.ecommerceanalyzer.resources;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.sql.SQLException;

@RestController
@EnableAutoConfiguration
public class ExecuteJobs {

    @GetMapping("/test")
    public String getString() {
        return "hello";
    }
}
