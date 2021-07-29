package com.westminster.ecommerceanalyzer.entities;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface AnalyzerJobRepo extends JpaRepository<AnalyzeJobEntity, Integer> {
    public AnalyzeJobEntity findAnalyzeJobEntityByAnalyzerNameAndLatestIsTrue(String analyzerName);
}
