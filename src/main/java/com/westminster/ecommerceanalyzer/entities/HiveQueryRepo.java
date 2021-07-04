package com.westminster.ecommerceanalyzer.entities;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

public interface HiveQueryRepo extends JpaRepository<HiveQueryEntity, Integer> {
    @Query("select hq from HiveQueryEntity hq where hq.name=?1 and hq.isDML=?2")
    HiveQueryEntity findByNameAndAndDML(String name, boolean isDML);
}
