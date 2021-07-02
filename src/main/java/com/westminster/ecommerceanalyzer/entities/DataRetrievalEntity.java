package com.westminster.ecommerceanalyzer.entities;

import javax.persistence.*;
import java.io.Serializable;
import java.sql.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;

@Entity
@Table(name = "data_retrieval")
public class DataRetrievalEntity implements Serializable {

    @Id
    @GeneratedValue( strategy = GenerationType.AUTO )
    private Long id;

    private int status;
    private Date date;

    public DataRetrievalEntity(Long id, int status, Date date) {
        this.id = id;
        this.status = status;
        this.date = date;
    }

    public DataRetrievalEntity() {
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getId() {
        return id;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public java.util.Date getDate() throws ParseException {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        String dateString = format.format(date);
        java.util.Date date1 = format.parse (dateString);
        return date;
    }

    public void setDate(java.util.Date date) throws ParseException {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        String dateString = format.format(date);
        Date date1 = (Date) format.parse (dateString);
        this.date = date1;
    }
}