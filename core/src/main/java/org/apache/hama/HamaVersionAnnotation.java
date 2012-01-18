package org.apache.hama;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * An attribute that 
 * holds the information on version and build information of a
 * Hama package that a potential user is working with.
 */

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.PACKAGE)

public @interface HamaVersionAnnotation {

  /**
   * Get the Hama version
   * @return the version string "0.3.3-dev"
   */
  String version();

  /**
   * Get the username that compiled Hama.
   */
  String user();

  /**
   * Get the date when Hama was compiled.
   * @return the date in unix 'date' format
   */
  String date();

  /**
   * Get the url for the subversion repository.
   */
  String url();

  /**
   * Get the subversion revision.
   * @return the revision number as a string (eg. "451451")
   */
  String revision();

  /**
   * Get the branch from which this was compiled.
   * @return The branch name, e.g. "trunk" or "branches/branch-0.20"
   */
  String branch();

  /**
   * Get a checksum of the source files from which
   * Hama was compiled.
   * @return a string that uniquely identifies the source
   **/
  String srcChecksum(); 	



}
