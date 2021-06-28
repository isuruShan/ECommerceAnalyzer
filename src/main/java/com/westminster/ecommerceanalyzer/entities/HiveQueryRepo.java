package com.westminster.ecommerceanalyzer.entities;

import org.springframework.data.jpa.repository.JpaRepository;

public interface HiveQueryRepo extends JpaRepository<HiveQueryEntity, Integer> {
    HiveQueryEntity findByName(String name);
}
