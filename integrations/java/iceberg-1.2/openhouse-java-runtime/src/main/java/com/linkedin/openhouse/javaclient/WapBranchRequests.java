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
 * (DROP / RENAME / GRANT / LOCK) when the target is not {@code main}. Header is the PoC wire until
 * OpenAPI grows a {@code branch} query param. GRANT also copies the same name into the ACL body
 * {@code properties} map — that is the request shape a later branch/clone ACL model consumes.
 */
final class WapBranchRequests {

  static final String HEADER = "X-OH-Wap-Branch";
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
      return next.exchange(ClientRequest.from(request).header(HEADER, branch).build());
    };
  }
}
