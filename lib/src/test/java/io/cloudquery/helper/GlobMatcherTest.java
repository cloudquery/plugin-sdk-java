package io.cloudquery.helper;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class GlobMatcherTest {
    @Test
    public void shouldMatchWildcard() {
        GlobMatcher globMatcher = new GlobMatcher("*");

        assertTrue(globMatcher.matches("aws_ec2_vpc"));
        assertTrue(globMatcher.matches("aws_ec2_eip"));
        assertTrue(globMatcher.matches("aws_ec2_instance"));
    }

    @Test
    public void shouldMatchWildcardSuffix() {
        GlobMatcher globMatcher = new GlobMatcher("aws_*");

        assertTrue(globMatcher.matches("aws_ec2_vpc"));
        assertTrue(globMatcher.matches("aws_ec2_eip"));
        assertTrue(globMatcher.matches("aws_ec2_instance"));

        assertFalse(globMatcher.matches("gcp_project"));
        assertFalse(globMatcher.matches("other_aws_resource"));
    }

    @Test
    public void shouldMatchWildcardPrefixAndSuffix() {
        GlobMatcher globMatcher = new GlobMatcher("*ec2*");

        assertTrue(globMatcher.matches("aws_ec2_vpc"));
        assertTrue(globMatcher.matches("aws_ec2_eip"));
        assertTrue(globMatcher.matches("aws_ec2_instance"));

        assertFalse(globMatcher.matches("gcp_project"));
        assertFalse(globMatcher.matches("other_aws_resource"));
    }
}