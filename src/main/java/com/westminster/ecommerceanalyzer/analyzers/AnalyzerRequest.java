package com.westminster.ecommerceanalyzer.analyzers;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.sql.Connection;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class AnalyzerRequest {
    private Connection con;
    private String sql;
}
