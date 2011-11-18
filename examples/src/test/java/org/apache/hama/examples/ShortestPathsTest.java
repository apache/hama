package org.apache.hama.examples;

import org.junit.Test;

import static org.junit.Assert.fail;

/**
 * @author tommaso
 */
public class ShortestPathsTest {

  @Test
  public void testShortestPathsWithWrongArgs() {
    try {
      ShortestPaths.main(new String[]{"1", ".", "."});
      fail("ShortestPaths should fail if the arguments list contains wrong items");
    } catch (Exception e) {
      // everything ok
    }
  }
}
