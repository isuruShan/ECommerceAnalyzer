package com.westminster.ecommerceanalyzer.entities;

import com.westminster.ecommerceanalyzer.models.AnalyzerStatus;

import javax.persistence.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.stream.Collectors;

@Entity
@Table(name="analyze_job")
public class AnalyzeJob {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long id;
    @Column(name = "analyzer_name")
    private String analyzerName;
    @Column(name = "date_triggered")
    private Date dateTriggered;
    @Column(name = "is_latest")
    private boolean isLatest;
    private int status;
    private String result;

    public void setId(Long id) {
        this.id = id;
    }
    public Long getId() {
        return id;
    }

    public String getAnalyzerName() {
        return analyzerName;
    }

    public void setAnalyzerName(String analyzerName) {
        this.analyzerName = analyzerName;
    }

    public java.util.Date getDateTriggered() throws ParseException {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        String dateString = format.format(dateTriggered);
        java.util.Date date1 = format.parse (dateString);
        return date1;
    }

    public void setDateTriggered(java.util.Date date) throws ParseException {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        String dateString = format.format(date);
        java.sql.Date date1 = (java.sql.Date) format.parse (dateString);
        this.dateTriggered = date1;
    }

    public boolean isLatest() {
        return isLatest;
    }

    public void setLatest(boolean latest) {
        isLatest = latest;
    }

    public AnalyzerStatus getStatus() {
        return Arrays.stream(AnalyzerStatus.values())
                .filter(analyzerStatus -> analyzerStatus.getValue() == this.status)
                .collect(Collectors.toList()).get(1);
    }

    public void setStatus(AnalyzerStatus status) {
        this.status = status.getValue();
    }

    public String getResult() {
        return result;
    }

    public void setResult(String result) {
        this.result = result;
    }
}
