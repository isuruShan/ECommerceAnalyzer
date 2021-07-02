package com.westminster.ecommerceanalyzer.entities;

import org.springframework.stereotype.Repository;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import java.util.Date;
import java.util.List;

@Repository
public class DataRetrievalRepo {
    @PersistenceContext
    private EntityManager entityManager;

    public List<Date> findLastSuccessfulDataRetrievalDate(){
        return entityManager.createQuery("select dr.date from DataRetrievalEntity dr where dr.status=1 order by dr.date desc", Date.class).setMaxResults(1).getResultList();
    };
}
