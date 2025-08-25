package com.linkedin.openhouse.tables.e2e.h2;

import com.linkedin.openhouse.internal.catalog.repository.HouseTableRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

/**
 * Test configuration that enables H2-based repository implementations for integration tests. This
 * configuration should be imported using @Import in tests that need H2 repository support.
 */
@TestConfiguration
@EnableJpaRepositories(basePackages = "com.linkedin.openhouse.tables.e2e.h2")
public class H2RepositoryTestConfiguration {

  /**
   * Make the HouseTablesH2Repository the primary bean to override production
   * HouseTableRepositoryImpl
   */
  @Bean
  @Primary
  public HouseTableRepository primaryHouseTableRepository(
      @Autowired HouseTablesH2Repository h2Repository) {
    return h2Repository;
  }
}
