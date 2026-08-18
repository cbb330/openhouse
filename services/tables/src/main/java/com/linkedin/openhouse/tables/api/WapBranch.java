package com.linkedin.openhouse.tables.api;

import com.linkedin.openhouse.tables.api.spec.v0.request.UpdateAclPoliciesRequestBody;
import org.springframework.web.context.request.RequestAttributes;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

/**
 * Session-branch target stamped by the OpenHouse Java client ({@code X-OH-Wap-Branch}, and for ACL
 * the body {@code properties.branch}). When the target is a non-main branch, identity mutations
 * (delete, rename, ACL, lock) are no-ops on the house table, and schema / user-property / policy /
 * sort updates and partition-spec eligibility are skipped on {@code updateTable} / {@code
 * putSnapshots}. Snapshots and refs still apply. The request still names the branch so a later
 * clone/branch ACL model can consume the same shape.
 */
public final class WapBranch {

  public static final String HEADER = "X-OH-Wap-Branch";
  public static final String ACL_PROPERTY = "branch";

  private WapBranch() {}

  public static String fromRequest() {
    RequestAttributes attrs = RequestContextHolder.getRequestAttributes();
    if (!(attrs instanceof ServletRequestAttributes)) {
      return null;
    }
    return blankToNull(((ServletRequestAttributes) attrs).getRequest().getHeader(HEADER));
  }

  public static String fromAcl(UpdateAclPoliciesRequestBody body) {
    if (body == null || body.getProperties() == null) {
      return null;
    }
    return blankToNull(body.getProperties().get(ACL_PROPERTY));
  }

  public static boolean isNonMain(String branch) {
    return branch != null && !branch.isEmpty() && !"main".equals(branch);
  }

  public static boolean shouldDrop(String... candidates) {
    if (candidates == null) {
      return false;
    }
    for (String candidate : candidates) {
      if (isNonMain(candidate)) {
        return true;
      }
    }
    return false;
  }

  private static String blankToNull(String value) {
    if (value == null) {
      return null;
    }
    String trimmed = value.trim();
    return trimmed.isEmpty() ? null : trimmed;
  }
}
