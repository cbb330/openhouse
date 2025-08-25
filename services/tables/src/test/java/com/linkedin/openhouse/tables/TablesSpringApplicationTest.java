package com.linkedin.openhouse.tables;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.zaxxer.hikari.HikariDataSource;
import javax.persistence.EntityManagerFactory;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.data.jpa.JpaRepositoriesAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

@SpringBootTest(classes = TablesSpringApplication.class)
@SpringJUnitConfig
class TablesSpringApplicationTest {

  @Test
  void testJpaRepositoriesAutoConfigurationIsNotActive(ApplicationContext context) {
    assertFalse(
        context.getBeansOfType(JpaRepositoriesAutoConfiguration.class).size() > 0,
        "JpaRepositoriesAutoConfiguration should not be active");
  }

  @Test
  void testDataSourceAutoConfigurationIsNotActive(ApplicationContext context) {
    assertFalse(
        context.getBeansOfType(DataSourceAutoConfiguration.class).size() > 0,
        "DataSourceAutoConfiguration should not be active");
  }

  @Test
  void testDataSourceTransactionManagerAutoConfigurationIsNotActive(ApplicationContext context) {
    assertFalse(
        context.getBeansOfType(DataSourceTransactionManagerAutoConfiguration.class).size() > 0,
        "DataSourceTransactionManagerAutoConfiguration should not be active");
  }

  @Test
  void testHibernateJpaAutoConfigurationIsNotActive(ApplicationContext context) {
    assertFalse(
        context.getBeansOfType(HibernateJpaAutoConfiguration.class).size() > 0,
        "HibernateJpaAutoConfiguration should not be active");
  }

  @Test
  void testNoDataSourceBeansPresent(ApplicationContext context) {
    assertFalse(
        context.getBeansOfType(DataSource.class).size() > 0,
        "No DataSource beans should be present");
  }

  @Test
  void testNoEntityManagerFactoryBeansPresent(ApplicationContext context) {
    assertFalse(
        context.getBeansOfType(EntityManagerFactory.class).size() > 0,
        "No EntityManagerFactory beans should be present");
  }

  @Test
  void testNoJdbcTemplateBeansPresent(ApplicationContext context) {
    assertFalse(
        context.getBeansOfType(JdbcTemplate.class).size() > 0,
        "No JdbcTemplate beans should be present");
  }

  @Test
  void testNoJpaTransactionManagerBeansPresent(ApplicationContext context) {
    assertFalse(
        context.getBeansOfType(JpaTransactionManager.class).size() > 0,
        "No JpaTransactionManager beans should be present");
  }

  @Test
  void testNoHikariDataSourceBeansPresent(ApplicationContext context) {
    assertFalse(
        context.getBeansOfType(HikariDataSource.class).size() > 0,
        "No HikariDataSource beans should be present");
  }

  @Test
  void testNoDatabaseRelatedBeansInContext(ApplicationContext context) {
    // Verify common database-related bean names are not present
    String[] problematicBeanNames = {
      "dataSource",
      "entityManagerFactory",
      "transactionManager",
      "jdbcTemplate",
      "entityManager",
      "jpaTransactionManager",
      "dataSourceTransactionManager",
      "spring.jpa-org.springframework.boot.autoconfigure.orm.jpa.JpaProperties",
      "spring.datasource-org.springframework.boot.autoconfigure.jdbc.DataSourceProperties"
    };

    for (String beanName : problematicBeanNames) {
      assertFalse(
          context.containsBean(beanName),
          "Bean '" + beanName + "' should not be present in context");
    }
  }

  @Test
  void testApplicationContextStartsSuccessfully(ApplicationContext context) {
    // Simple test to ensure the application context loads without database dependencies
    assertTrue(context != null, "Application context should start successfully");
    assertTrue(context.getBeanDefinitionCount() > 0, "Application context should contain beans");
  }
}
