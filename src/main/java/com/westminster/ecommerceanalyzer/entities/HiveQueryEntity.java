package com.westminster.ecommerceanalyzer.entities;

import javax.persistence.*;
import java.io.Serializable;

@Entity
@Table(name = "hive_query")
public class HiveQueryEntity implements Serializable {
    @Id
    @GeneratedValue( strategy = GenerationType.AUTO )
    private int id;
    private String name;
    private String query;

    public HiveQueryEntity(int id, String name, String query) {
        this.id = id;
        this.name = name;
        this.query = query;
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
}
