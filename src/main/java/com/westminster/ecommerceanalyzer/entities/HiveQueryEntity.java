package com.westminster.ecommerceanalyzer.entities;

import javax.persistence.*;
import java.io.Serializable;

@Entity
@Table(name = "hive_query")
public class HiveQueryEntity implements Serializable {
    @Id
    @GeneratedValue( strategy = GenerationType.IDENTITY )
    private int id;
    private String name;
    private String query;
    @Column(name="is_dml")
    private boolean isDML;

    public HiveQueryEntity(int id, String name, String query, boolean isDML) {
        this.id = id;
        this.name = name;
        this.query = query;
        this.isDML = isDML;
    }

    public HiveQueryEntity() {
    }

    public void setId(int id) {
        this.id = id;
    }
    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public boolean isDML() {
        return isDML;
    }

    public void setDML(boolean DML) {
        isDML = DML;
    }
}
