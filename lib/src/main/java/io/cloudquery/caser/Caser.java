package io.cloudquery.caser;

import static io.cloudquery.caser.Initialisms.*;
import static io.cloudquery.caser.Initialisms.COMMON_INITIALISMS;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.Builder;

@Builder
public class Caser {
  @Builder.Default private Set<String> initialisms = new HashSet<>(COMMON_INITIALISMS);

  @Builder.Default private Map<String, String> snakeToCamelExceptions = new HashMap<>();

  @Builder.Default private Map<String, String> camelToSnakeExceptions = new HashMap<>();

  @Builder.Default private Map<String, String> customExceptions = new HashMap<>();

  @Builder.Default private Set<String> customInitialisms = new HashSet<>();

  public Caser(
      Set<String> initialisms,
      Map<String, String> snakeToCamelExceptions,
      Map<String, String> camelToSnakeExceptions,
      Map<String, String> customExceptions,
      Set<String> customInitialisms) {
    this.initialisms = initialisms;
    this.snakeToCamelExceptions = snakeToCamelExceptions;
    this.camelToSnakeExceptions = camelToSnakeExceptions;
    this.customExceptions = customExceptions;
    this.customInitialisms = customInitialisms;

    HashMap<String, String> combinedExceptions = new HashMap<>(COMMON_EXCEPTIONS);
    combinedExceptions.putAll(customExceptions);
    for (String key : combinedExceptions.keySet()) {
      snakeToCamelExceptions.put(key, combinedExceptions.get(key));
      camelToSnakeExceptions.put(combinedExceptions.get(key), key);
    }

    initialisms.addAll(customInitialisms);
  }

  public String toSnake(String s) {
    List<String> words = new ArrayList<>();
    int lastPos = 0;
    for (int i = 0; i < s.length(); i++) {
      if (i > 0 && Character.isUpperCase(s.charAt(i))) {

        String initialism = startsWithInitialism(s.substring(lastPos));
        if (!initialism.isEmpty()) {
          words.add(initialism);
          i = lastPos + initialism.length();
          lastPos = i;
          continue;
        }

        String capWord = getCapWord(s.substring(lastPos));
        if (!capWord.isEmpty()) {
          words.add(capWord);
          i = lastPos + capWord.length();
          lastPos = i;
          continue;
        }

        words.add(s.substring(lastPos, i));
        lastPos = i;
      }
    }

    if (!s.substring(lastPos).isEmpty()) {
      String w = s.substring(lastPos);
      if (w.equals("s")) {
        String lastWord = words.remove(words.size() - 1);
        words.add(lastWord + w);
      } else {
        words.add(s.substring(lastPos));
      }
    }

    return String.join("_", words).toLowerCase();
  }

  /**
   * Returns a string converted from snake case to camel case.
   *
   * <p>
   *
   * @param s The input string
   * @return The string converted to camel case
   */
  public String toCamel(String s) {
    if (s.isEmpty()) {
      return s;
    }
    List<String> words = Arrays.asList(s.split("_"));
    return String.join("", capitalize(words));
  }

  /**
   * Returns a string converted from snake case to title case.
   *
   * <p>Title case is similar to camel case, but spaces are used in between words.
   *
   * @param s The input string
   * @return The string converted to title case
   */
  public String toTitle(String s) {
    if (s.isEmpty()) {
      return s;
    }
    String[] words = s.split("_");
    if (!snakeToCamelExceptions.containsKey(words[0].toLowerCase())) {
      words[0] = words[0].substring(0, 1).toUpperCase() + words[0].substring(1).toLowerCase();
    }
    return String.join(" ", capitalize(Arrays.asList(words)));
  }

  /**
   * Returns a string converted from snake case to pascal case
   *
   * @param s The input string
   * @return The string converted to pascal case
   */
  public String toPascal(String s) {
    if (s.isEmpty()) {
      return s;
    }
    String camel = toCamel(s);
    return camel.substring(0, 1).toUpperCase() + camel.substring(1);
  }

  /**
   * gets the next sequence of capitalized letters as a single word.
   *
   * <p>If there is a word after capitalized sequence it leaves one letter as beginning of the next
   * word
   *
   * @param s The input string
   * @return A single word
   */
  private String getCapWord(String s) {
    for (int i = 0; i < s.length(); i++) {
      if (!Character.isUpperCase(s.charAt(i))) {
        if (i == 0) {
          return "";
        }
        return s.substring(0, i - 1);
      }
    }
    return s;
  }

  /**
   * Returns the initialism if the given string begins with it
   *
   * @param s The input string
   * @return The initialism if the given string begins with it, otherwise an empty string
   */
  private String startsWithInitialism(String s) {
    String initialism = "";

    // the longest initialism is 5 char, the shortest 2 we choose the longest match
    for (int i = 1; i <= s.length() && i <= 5; i++) {
      if (s.length() > i - 1
          && this.initialisms.contains(s.substring(0, i))
          && s.substring(0, i).length() > initialism.length()) {
        initialism = s.substring(0, i);
      }
    }

    return initialism;
  }

  private List<String> capitalize(List<String> words) {
    int n = words.stream().map(String::length).reduce(0, Integer::sum);

    List<String> results = new ArrayList<>();
    for (int i = 0; i < words.size(); i++) {
      if (snakeToCamelExceptions.containsKey(words.get(i))) {
        results.add(snakeToCamelExceptions.get(words.get(i)));
        continue;
      }

      if (i > 0) {
        String upper = words.get(i).toUpperCase();
        if (n > i - 1 && initialisms.contains(upper)) {
          results.add(upper);
          continue;
        }
      }

      if (i > 0 && !words.get(i).isEmpty()) {
        results.add(words.get(i).substring(0, 1).toUpperCase() + words.get(i).substring(1));
      } else {
        results.add(words.get(i));
      }
    }
    return results;
  }
}
