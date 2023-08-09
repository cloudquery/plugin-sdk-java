package io.cloudquery.glob;

import org.junit.jupiter.api.Test;

import java.util.List;

import static io.cloudquery.glob.Glob.GLOB;
import static org.junit.jupiter.api.Assertions.*;

public class GlobTest {
    @Test
    public void testEmptyPattern() {
        assertGlobMatch("", "");
        assertNotGlobMatch("", "test");
    }

    @Test
    public void testEmptySubject() {
        for (String s : List.of("",
                "*",
                "**",
                "***",
                "****************",
                GLOB.repeat(1000000)
        )) {
            assertGlobMatch(s, "");
        }

        for (String pattern : List.of(
                // No globs/non-glob characters
                "test",
                "*test*",

                // Trailing characters
                "*x",
                "*****************x",
                GLOB.repeat(1000000) + "x",

                // Leading characters
                "x*",
                "x*****************",
                "x" + GLOB.repeat(1000000),

                // Mixed leading/trailing characters
                "x*x",
                "x****************x",
                "x" + GLOB.repeat(1000000) + "x"
        )) {
            assertNotGlobMatch(pattern, "");
        }
    }

    @Test
    public void testPatternWithoutGlobs() {
        assertGlobMatch("test", "test");
    }

    @Test
    public void testGlobs() {
        for (String pattern : List.of(
                "*test",           // Leading glob
                "this*",           // Trailing glob
                "this*test",       // Middle glob
                "*is *",           // String in between two globs
                "*is*a*",          // Lots of globs
                "**test**",        // Double glob characters
                "**is**a***test*", // Varying number of globs
                "* *",             // White space between globs
                "*",               // Lone glob
                "**********",      // Nothing but globs
                "*Ѿ*",             // Unicode with globs
                "*is a ϗѾ *"       // Mixed ASCII/unicode
        )) {
            assertGlobMatch(pattern, "this is a ϗѾ test");
        }

        for (String pattern : List.of(
                "test*",               // Implicit substring match
                "*is",                 // Partial match
                "*no*",                // Globs without a match between them
                " ",                   // Plain white space
                "* ",                  // Trailing white space
                " *",                  // Leading white space
                "*ʤ*",                 // Non-matching unicode
                "this*this is a test"  // Repeated prefix
        )) {
            assertNotGlobMatch(pattern, "this is a test");
        }
    }

    public void assertGlobMatch(String pattern, String subject) {
        assertTrue(Glob.match(pattern, subject), String.format("\"%s\" should match \"%s\"", pattern, subject));
    }

    public void assertNotGlobMatch(String pattern, String subject) {
        assertFalse(Glob.match(pattern, subject), String.format("\"%s\" should not match \"%s\"", pattern, subject));
    }

}