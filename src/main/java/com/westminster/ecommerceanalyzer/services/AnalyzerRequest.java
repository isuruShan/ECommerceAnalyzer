package com.westminster.ecommerceanalyzer.services;

import com.westminster.ecommerceanalyzer.models.HiveQueryNames;
import lombok.*;

import java.sql.Connection;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class AnalyzerRequest {
    @NonNull
    private HiveQueryNames queryName;
    @NonNull
    private Connection con;
    @NonNull
    private String sql;
}
