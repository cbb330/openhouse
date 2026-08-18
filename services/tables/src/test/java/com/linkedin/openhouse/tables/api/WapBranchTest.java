package com.linkedin.openhouse.tables.api;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.linkedin.openhouse.tables.api.spec.v0.request.UpdateAclPoliciesRequestBody;
import java.util.Collections;
import org.junit.jupiter.api.Test;

class WapBranchTest {

  @Test
  void nonMainIsDropped() {
    assertTrue(WapBranch.isNonMain("ci"));
    assertTrue(WapBranch.shouldDrop("ci"));
    assertTrue(WapBranch.shouldDrop(null, "ci"));
  }

  @Test
  void mainAndBlankAreNotDropped() {
    assertFalse(WapBranch.isNonMain("main"));
    assertFalse(WapBranch.isNonMain(null));
    assertFalse(WapBranch.isNonMain(""));
    assertFalse(WapBranch.shouldDrop("main"));
    assertFalse(WapBranch.shouldDrop((String) null));
  }

  @Test
  void aclBodyBranchIsDropped() {
    UpdateAclPoliciesRequestBody body =
        UpdateAclPoliciesRequestBody.builder()
            .role("TABLE_VIEWER")
            .principal("ci_principal")
            .operation(UpdateAclPoliciesRequestBody.Operation.GRANT)
            .properties(Collections.singletonMap(WapBranch.ACL_PROPERTY, "ci"))
            .build();
    assertTrue(WapBranch.shouldDrop(WapBranch.fromAcl(body)));
  }
}
