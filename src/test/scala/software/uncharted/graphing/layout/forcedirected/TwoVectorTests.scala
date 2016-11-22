/**
  * Copyright (c) 2014-2016 Uncharted Software Inc. All rights reserved.
  *
  * Property of Uncharted(tm), formerly Oculus Info Inc.
  * http://uncharted.software/
  *
  * This software is the confidential and proprietary information of
  * Uncharted Software Inc. ("Confidential Information"). You shall not
  * disclose such Confidential Information and shall use it only in
  * accordance with the terms of the license agreement you entered into
  * with Uncharted Software Inc.
  */
package software.uncharted.graphing.layout.forcedirected

import org.scalatest.FunSpec
import software.uncharted.graphing.layout.V2

class TwoVectorTests extends FunSpec {
  describe("affine two-vectors") {
    it("should equal an equivalent two-vector") {
      assert(V2( 1.0,  3.0) === V2( 1.0,  3.0))
      assert(V2(-1.0,  3.0) === V2(-1.0,  3.0))
      assert(V2( 1.0, -3.0) === V2( 1.0, -3.0))
      assert(V2(-1.0, -3.0) === V2(-1.0, -3.0))
      assert(V2( 0.0,  3.0) === V2( 0.0,  3.0))
      assert(V2( 1.0,  0.0) === V2( 1.0,  0.0))
      assert(V2( 0.0,  0.0) === V2( 0.0,  0.0))
    }
  }
}
