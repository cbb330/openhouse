package com.linkedin.openhouse.tables;

import static org.junit.jupiter.api.Assertions.assertFalse;

import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.data.jpa.JpaRepositoriesAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
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
}
