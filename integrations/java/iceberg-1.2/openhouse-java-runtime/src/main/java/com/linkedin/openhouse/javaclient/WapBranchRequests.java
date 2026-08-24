package com.linkedin.openhouse.javaclient;

import com.linkedin.openhouse.tables.client.invoker.ApiClient;
import java.util.HashMap;
import java.util.Map;
import org.springframework.http.HttpHeaders;
import org.springframework.web.reactive.function.client.ClientRequest;
import org.springframework.web.reactive.function.client.ExchangeFilterFunction;
import org.springframework.web.reactive.function.client.WebClient;

/**
 * Stamps the session branch onto every Tables REST call so the server can no-op identity mutations
 * (DROP / RENAME / GRANT / LOCK) when the target is not {@code main}.
 *
 * <p>{@code X-OH-Wap-Branch} is today's OpenHouse header (request body unchanged). {@code
 * X-Iceberg-Ref} is the same value as an Iceberg REST 1.11-shaped alias: REST 1.11 carries {@code
 * ref} on commit requirements / {@code ref-name} on snapshot-ref updates, not a load-table header.
 * Extra headers are ignored by Iceberg REST, so both can ship until OpenHouse speaks that commit
 * body. GRANT also copies the name into the ACL body {@code properties} map.
 */
final class WapBranchRequests {

  static final String HEADER = "X-OH-Wap-Branch";
  static final String ICEBERG_REF_HEADER = "X-Iceberg-Ref";
  static final String ACL_PROPERTY = "branch";

  private WapBranchRequests() {}

  static boolean isNonMain(String branch) {
    return branch != null && !branch.isEmpty() && !"main".equals(branch);
  }

  static ApiClient stamp(ApiClient client, String baseUrl, String token) {
    WebClient stamped = client.getWebClient().mutate().filter(headerFilter()).build();
    ApiClient next = new ApiClient(stamped);
    if (token != null && !token.isEmpty()) {
      next.addDefaultHeader(HttpHeaders.AUTHORIZATION, String.format("Bearer %s", token));
    }
    next.setBasePath(baseUrl);
    return next;
  }

  static Map<String, String> aclProperties() {
    String branch = SessionWapBranch.get();
    if (!isNonMain(branch)) {
      return null;
    }
    Map<String, String> props = new HashMap<>();
    props.put(ACL_PROPERTY, branch);
    return props;
  }

  static ExchangeFilterFunction headerFilter() {
    return (request, next) -> {
      String branch = SessionWapBranch.get();
      if (!isNonMain(branch)) {
        return next.exchange(request);
      }
      return next.exchange(
          ClientRequest.from(request)
              .header(HEADER, branch)
              .header(ICEBERG_REF_HEADER, branch)
              .build());
    };
  }
}
