package com.linkedin.openhouse.internal.catalog.utils;

import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.RetryListener;
import org.springframework.retry.backoff.BackOffContext;
import org.springframework.retry.backoff.BackOffPolicy;
import org.springframework.retry.backoff.BackOffPolicyBuilder;
import org.springframework.retry.policy.CompositeRetryPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.policy.TimeoutRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

/** Common utilities for retrying operations in the internal catalog. */
@Slf4j
public final class RetryUtils {
  private RetryUtils() {
    // utilities class private ctor noop
  }

  /**
   * Custom retry listener that logs comprehensive retry information including: - Number of retries
   * - Time taken for each retry - Backoff delays - Failed retry attempts - Operation-specific
   * context (e.g., metadata location)
   */
  private static class DetailedRetryListener implements RetryListener {
    private final String operationName;

    DetailedRetryListener(String operationName) {
      this.operationName = operationName;
    }

    @Override
    public <T, E extends Throwable> boolean open(
        RetryContext context, RetryCallback<T, E> callback) {
      context.setAttribute("startTime", System.currentTimeMillis());
      return true;
    }

    @Override
    public <T, E extends Throwable> void close(
        RetryContext context, RetryCallback<T, E> callback, Throwable throwable) {
      long startTime = (Long) context.getAttribute("startTime");
      long totalTime = System.currentTimeMillis() - startTime;
      int retryCount = context.getRetryCount();
      String location = (String) context.getAttribute("location");

      // Build location context string if location is provided
      String locationContext = location != null ? " from location " + location : "";

      if (throwable != null) {
        // Log detailed retry info
        log.error(
            "{} operation{} failed after {} attempts ({} retries) over {} ms. Final error: {}",
            operationName,
            locationContext,
            retryCount,
            Math.max(0, retryCount - 1),
            totalTime,
            throwable.getMessage(),
            throwable);
      } else {
        // Log success with retry info if retries occurred
        if (retryCount > 1) {
          log.info(
              "{} operation{} succeeded after {} attempts ({} retries) over {} ms",
              operationName,
              locationContext,
              retryCount,
              retryCount - 1,
              totalTime);
        } else {
          log.info(
              "{} operation{} succeeded, took {} ms", operationName, locationContext, totalTime);
        }
      }
    }

    @Override
    public <T, E extends Throwable> void onError(
        RetryContext context, RetryCallback<T, E> callback, Throwable throwable) {
      int attemptNumber = context.getRetryCount();
      long attemptStartTime =
          context.getAttribute("attemptStartTime") != null
              ? (Long) context.getAttribute("attemptStartTime")
              : (Long) context.getAttribute("startTime");
      long attemptDuration = System.currentTimeMillis() - attemptStartTime;

      // Calculate next backoff delay if available
      Object backoffContext = context.getAttribute(BackOffContext.class.getName());
      String nextRetryInfo = "";
      if (backoffContext != null && !context.isExhaustedOnly()) {
        nextRetryInfo = " Will retry shortly with exponential backoff.";
      }

      log.warn(
          "{} operation failed on attempt {} after {} ms. Error: {}.{}",
          operationName,
          attemptNumber,
          attemptDuration,
          throwable.getMessage(),
          nextRetryInfo);

      // Store start time for next attempt
      context.setAttribute("attemptStartTime", System.currentTimeMillis());
    }
  }

  // HTS retry configuration (existing behavior)
  public static final int MAX_RETRY_ATTEMPT = 3;

  public static final BackOffPolicy DEFAULT_HTS_BACKOFF_POLICY =
      BackOffPolicyBuilder.newBuilder()
          .multiplier(2.0)
          .delay(TimeUnit.SECONDS.toMillis(2))
          .maxDelay(TimeUnit.SECONDS.toMillis(30))
          .build();

  // Refresh metadata retry configuration
  private static final int REFRESH_METADATA_MAX_ATTEMPTS = 5;
  private static final long REFRESH_METADATA_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(45);

  private static final BackOffPolicy REFRESH_METADATA_BACKOFF_POLICY =
      BackOffPolicyBuilder.newBuilder()
          .multiplier(2.0)
          .delay(TimeUnit.SECONDS.toMillis(1))
          .maxDelay(TimeUnit.SECONDS.toMillis(20))
          .build();

  // Update metadata retry configuration
  private static final int UPDATE_METADATA_MAX_ATTEMPTS = 3;
  private static final long UPDATE_METADATA_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(60);

  private static final BackOffPolicy UPDATE_METADATA_BACKOFF_POLICY =
      BackOffPolicyBuilder.newBuilder()
          .multiplier(2.0)
          .delay(TimeUnit.SECONDS.toMillis(2))
          .maxDelay(TimeUnit.SECONDS.toMillis(30))
          .build();

  /**
   * Get a retry template for HTS operations. This maintains the existing HTS retry behavior.
   *
   * @return RetryTemplate configured for HTS operations
   */
  public static RetryTemplate getHtsRetryTemplate() {
    return RetryTemplate.builder()
        .maxAttempts(MAX_RETRY_ATTEMPT)
        .customBackoff(DEFAULT_HTS_BACKOFF_POLICY)
        .withListener(new DetailedRetryListener("HTS"))
        .build();
  }

  /**
   * Get a retry template for refresh metadata operations with timeout support.
   *
   * @return RetryTemplate configured for refresh metadata operations
   */
  public static RetryTemplate getRefreshMetadataRetryTemplate() {
    // Create timeout policy
    TimeoutRetryPolicy timeoutPolicy = new TimeoutRetryPolicy();
    timeoutPolicy.setTimeout(REFRESH_METADATA_TIMEOUT_MS);

    // Create max attempts policy
    SimpleRetryPolicy attemptsPolicy = new SimpleRetryPolicy();
    attemptsPolicy.setMaxAttempts(REFRESH_METADATA_MAX_ATTEMPTS);

    // Combine policies - retry until either timeout or max attempts is reached
    CompositeRetryPolicy compositePolicy = new CompositeRetryPolicy();
    compositePolicy.setPolicies(
        new org.springframework.retry.RetryPolicy[] {timeoutPolicy, attemptsPolicy});

    return RetryTemplate.builder()
        .customPolicy(compositePolicy)
        .customBackoff(REFRESH_METADATA_BACKOFF_POLICY)
        .withListener(new DetailedRetryListener("RefreshMetadata"))
        .build();
  }

  /**
   * Get a retry template for update metadata operations with timeout support.
   *
   * @return RetryTemplate configured for update metadata operations
   */
  public static RetryTemplate getUpdateMetadataRetryTemplate() {
    // Create timeout policy
    TimeoutRetryPolicy timeoutPolicy = new TimeoutRetryPolicy();
    timeoutPolicy.setTimeout(UPDATE_METADATA_TIMEOUT_MS);

    // Create max attempts policy
    SimpleRetryPolicy attemptsPolicy = new SimpleRetryPolicy();
    attemptsPolicy.setMaxAttempts(UPDATE_METADATA_MAX_ATTEMPTS);

    // Combine policies - retry until either timeout or max attempts is reached
    CompositeRetryPolicy compositePolicy = new CompositeRetryPolicy();
    compositePolicy.setPolicies(
        new org.springframework.retry.RetryPolicy[] {timeoutPolicy, attemptsPolicy});

    return RetryTemplate.builder()
        .customPolicy(compositePolicy)
        .customBackoff(UPDATE_METADATA_BACKOFF_POLICY)
        .withListener(new DetailedRetryListener("UpdateMetadata"))
        .build();
  }
}
