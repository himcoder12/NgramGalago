/*
 * ComputedStatistics
 * 
 * September 20, 2007
 * 
 * BSD License (http://www.galagosearch.org/license)
 */
package org.galagosearch.core.retrieval.structured;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 *
 * @author trevor
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
public @interface RequiredStatistics {
    public String[] statistics() default {};
}
