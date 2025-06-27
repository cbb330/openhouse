package com.linkedin.openhouse.tables.config;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.linkedin.openhouse.tables.TablesSpringApplication;
import java.util.Arrays;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;

/**
 * Test to verify that HikariCP-related auto-configurations are properly excluded from the
 * TablesSpringApplication to prevent unnecessary database connections.
 */
public class HikariCpMetricsTest {

  @Test
  public void testDataSourceAutoConfigurationIsNotExcluded() {
    // Get the @EnableAutoConfiguration annotation from TablesSpringApplication
    EnableAutoConfiguration annotation =
        TablesSpringApplication.class.getAnnotation(EnableAutoConfiguration.class);

    assertTrue(annotation != null, "TablesSpringApplication should have @EnableAutoConfiguration");

    // Verify that DataSourceAutoConfiguration is NOT excluded (we're testing if
    // HibernateJpaAutoConfiguration exclusion is sufficient)
    boolean hasDataSourceExclusion =
        Arrays.asList(annotation.exclude()).contains(DataSourceAutoConfiguration.class);

    assertFalse(
        hasDataSourceExclusion,
        "Testing if DataSourceAutoConfiguration exclusion is unnecessary when HibernateJpaAutoConfiguration is excluded");
  }

  @Test
  public void testHibernateJpaAutoConfigurationIsExcluded() {
    // Get the @EnableAutoConfiguration annotation from TablesSpringApplication
    EnableAutoConfiguration annotation =
        TablesSpringApplication.class.getAnnotation(EnableAutoConfiguration.class);

    assertTrue(annotation != null, "TablesSpringApplication should have @EnableAutoConfiguration");

    // Verify that HibernateJpaAutoConfiguration is excluded
    boolean hasHibernateExclusion =
        Arrays.asList(annotation.exclude()).contains(HibernateJpaAutoConfiguration.class);

    assertTrue(
        hasHibernateExclusion,
        "HibernateJpaAutoConfiguration should be excluded to prevent automatic DataSource creation");
  }

  @Test
  public void testOnlyHibernateJpaAutoConfigurationIsExcluded() {
    // Get the @EnableAutoConfiguration annotation from TablesSpringApplication
    EnableAutoConfiguration annotation =
        TablesSpringApplication.class.getAnnotation(EnableAutoConfiguration.class);

    // Check that only Hibernate JPA auto-configuration is excluded (testing if DataSource exclusion
    // is unnecessary)
    boolean excludesDataSource =
        Arrays.asList(annotation.exclude()).contains(DataSourceAutoConfiguration.class);
    boolean excludesHibernate =
        Arrays.asList(annotation.exclude()).contains(HibernateJpaAutoConfiguration.class);

    assertTrue(
        excludesHibernate && !excludesDataSource,
        "Only HibernateJpaAutoConfiguration should be excluded. Testing if this is sufficient "
            + "to prevent HikariCP connection pools without also excluding DataSourceAutoConfiguration");
  }
}
