package com.westminster.ecommerceanalyzer.analyzers;

import org.springframework.stereotype.Component;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

@Component
public class WeeklyIncomeAnalyzer {

    public long run(AnalyzerRequest analyzerRequest) throws SQLException {
        Statement statement = analyzerRequest.getCon().createStatement();
        ResultSet resultSet = statement.executeQuery(analyzerRequest.getSql());
        System.out.println("Result:");
//        System.out.println(" UserID \t MovieId \t Rating \t Time");
//
//        while (res.next()) {
//            System.out.printl|n(res.getInt(1) + " " + res.getString(2) + " " + res.getDouble(3) + " " + res.getString(4));
        return 1L;
    }

}
