package io.cloudquery.glob;

public class Glob {
  public static final String GLOB = "*";

  public static boolean match(String pattern, String subject) {
    if (pattern.isEmpty()) {
      return subject.equals(pattern);
    }

    if (pattern.equals(GLOB)) {
      return true;
    }

    String[] parts = pattern.split("\\" + GLOB, -1);
    if (parts.length == 1) {
      return subject.equals(pattern);
    }

    boolean leadingGlob = pattern.startsWith(GLOB);
    boolean trailingGlob = pattern.endsWith(GLOB);
    int end = parts.length - 1;

    for (int i = 0; i < end; i++) {
      int idx = subject.indexOf(parts[i]);

      if (i == 0) {
        if (!leadingGlob && idx != 0) {
          return false;
        }
      } else {
        if (idx < 0) {
          return false;
        }
      }

      subject = subject.substring(idx + parts[i].length());
    }

    return trailingGlob || subject.endsWith(parts[end]);
  }
}
