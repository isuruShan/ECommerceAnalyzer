package com.westminster.ecommerceanalyzer.resources;

import com.westminster.ecommerceanalyzer.models.AnalyzerException;
import com.westminster.ecommerceanalyzer.services.AnalyzersExecutorService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.ParseException;

@RestController
@EnableAutoConfiguration
public class ExecuteJobs {

    @Autowired
    AnalyzersExecutorService analyzersExecutorService;

    public static final String ANALYZER_EXECUTOR_ENDPOINT = "/execute-analyzer";

    @GetMapping(ANALYZER_EXECUTOR_ENDPOINT)
    public String getMostPopularSeller(@RequestParam String analyzer) throws AnalyzerException, ParseException {
        switch (analyzer) {
            case "most-popular-seller":
                analyzersExecutorService.executeMostPopularSellersAnalyzer();
                break;
            case "most-revenue-generating-location":
                analyzersExecutorService.executeMostRevenueLocationsAnalyzer();
                break;
            case "sales-percentages":
                analyzersExecutorService.executeWeeklyIncomeAnalyzer();
                break;
            case "most-sold-products-using-credit-cards":
                analyzersExecutorService.executeMostSoldCreditCardProductsAnalyzer();
                break;
            case "most-selling-products":
                analyzersExecutorService.executeLeastRevenueLocationsMostSellingProductsAnalyzer();
                break;
        }

        return "invoked the analyzer successfully.";
    }
}
