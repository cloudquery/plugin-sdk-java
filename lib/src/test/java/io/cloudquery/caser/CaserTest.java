package io.cloudquery.caser;

import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class CaserTest {
  public static Stream<Arguments> toSnakeSource() {
    return Stream.of(
        Arguments.of("TestCamelCase", "test_camel_case"),
        Arguments.of("TestCamelCase", "test_camel_case"),
        Arguments.of("AccountID", "account_id"),
        Arguments.of("IDs", "ids"),
        Arguments.of("PostgreSQL", "postgre_sql"),
        Arguments.of("QueryStoreRetention", "query_store_retention"),
        Arguments.of("TestCamelCaseLongString", "test_camel_case_long_string"),
        Arguments.of("testCamelCaseLongString", "test_camel_case_long_string"),
        Arguments.of("testIPv4", "test_ipv4"),
        Arguments.of("CoreIPs", "core_ips"),
        Arguments.of("CoreIps", "core_ips"),
        Arguments.of("CoreV1", "core_v1"),
        Arguments.of("APIVersion", "api_version"),
        Arguments.of("TTLSecondsAfterFinished", "ttl_seconds_after_finished"),
        Arguments.of("PodCIDRs", "pod_cidrs"),
        Arguments.of("IAMRoles", "iam_roles"),
        Arguments.of("testIAM", "test_iam"),
        Arguments.of("TestAWSMode", "test_aws_mode"));
  }

  public static Stream<Arguments> toCamelSource() {
    return Stream.of(
        Arguments.of("testCamelCase", "test_camel_case"),
        Arguments.of("accountID", "account_id"),
        Arguments.of("arns", "arns"),
        Arguments.of("postgreSQL", "postgre_sql"),
        Arguments.of("queryStoreRetention", "query_store_retention"),
        Arguments.of("testCamelCaseLongString", "test_camel_case_long_string"),
        Arguments.of("testCamelCaseLongString", "test_camel_case_long_string"),
        Arguments.of("testIPv4", "test_ipv4"));
  }

  public static Stream<Arguments> toTitleSource() {
    return Stream.of(
        Arguments.of("Test Camel Case", "test_camel_case"),
        Arguments.of("Account ID", "account_id"),
        Arguments.of("ARNs", "arns"),
        Arguments.of("Postgre SQL", "postgre_sql"),
        Arguments.of("Query Store Retention", "query_store_retention"),
        Arguments.of("Test Camel Case Long String", "test_camel_case_long_string"),
        Arguments.of("Test Camel Case Long String", "test_camel_case_long_string"),
        Arguments.of("Test IPv4", "test_ipv4"),
        Arguments.of("AWS Test Table", "aws_test_table"),
        Arguments.of("Gcp Test Table", "gcp_test_table"));
  }

  public static Stream<Arguments> toPascalSource() {
    return Stream.of(
        Arguments.of("TestCamelCase", "test_camel_case"),
        Arguments.of("AccountID", "account_id"),
        Arguments.of("Arns", "arns"),
        Arguments.of("PostgreSQL", "postgre_sql"),
        Arguments.of("QueryStoreRetention", "query_store_retention"),
        Arguments.of("TestCamelCaseLongString", "test_camel_case_long_string"),
        Arguments.of("TestCamelCaseLongString", "test_camel_case_long_string"),
        Arguments.of("TestV1", "test_v1"),
        Arguments.of("TestIPv4", "test_ipv4"),
        Arguments.of("Ec2", "ec2"),
        Arguments.of("S3", "s3"));
  }

  public static Stream<Arguments> inversionSource() {
    return Stream.of(
        Arguments.of("TestCamelCase"),
        Arguments.of("AccountID"),
        Arguments.of("Arns"),
        Arguments.of("PostgreSQL"),
        Arguments.of("QueryStoreRetention"),
        Arguments.of("TestCamelCaseLongString"),
        Arguments.of("TestCamelCaseLongString"),
        Arguments.of("TestV1"),
        Arguments.of("TestIPv4"),
        Arguments.of("TestIPv4"),
        Arguments.of("S3"));
  }

  public static Stream<Arguments> configureSource() {
    return Stream.of(
        Arguments.of("CDNs", "cdns"),
        Arguments.of("ARNs", "arns"),
        Arguments.of("EC2", "ec2"),
        Arguments.of("S3", "s3"));
  }

  public static Stream<Arguments> customExceptionsSource() {
    return Stream.of(Arguments.of("TEst", "test"), Arguments.of("TTv2", "ttv2"));
  }

  @ParameterizedTest
  @MethodSource("toSnakeSource")
  public void testToSnake(String camel, String snake) {
    Assertions.assertEquals(snake, Caser.builder().build().toSnake(camel));
  }

  @ParameterizedTest
  @MethodSource("toCamelSource")
  public void testToCamel(String camel, String snake) {
    Assertions.assertEquals(camel, Caser.builder().build().toCamel(snake));
  }

  @ParameterizedTest
  @MethodSource("toTitleSource")
  public void testToTitle(String title, String snake) {
    Caser caser =
        Caser.builder()
            .customExceptions(
                Map.of(
                    "arns", "ARNs",
                    "aws", "AWS"))
            .build();
    Assertions.assertEquals(title, caser.toTitle(snake));
  }

  @ParameterizedTest
  @MethodSource("toPascalSource")
  public void testToPascal(String pascal, String snake) {
    Caser caser = Caser.builder().build();
    Assertions.assertEquals(pascal, caser.toPascal(snake));
  }

  @ParameterizedTest
  @MethodSource("inversionSource")
  public void testInversion(String pascal) {
    Caser caser = Caser.builder().build();
    Assertions.assertEquals(pascal, caser.toPascal(caser.toSnake(pascal)));
  }

  @ParameterizedTest
  @MethodSource("configureSource")
  public void testConfigure(String camel, String snake) {
    Caser caser = Caser.builder().customInitialisms(Set.of("CDN", "ARN", "EC2")).build();
    Assertions.assertEquals(snake, caser.toSnake(camel));
  }

  @ParameterizedTest
  @MethodSource("customExceptionsSource")
  public void testCustomExceptions(String camel, String snake) {
    Caser caser = Caser.builder().customExceptions(Map.of("test", "TEst", "ttv2", "TTv2")).build();
    Assertions.assertEquals(camel, caser.toCamel(snake));
  }
}
