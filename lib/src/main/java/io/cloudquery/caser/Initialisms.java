package io.cloudquery.caser;

import java.util.Map;
import java.util.Set;

public class Initialisms {
  public static final Set<String> COMMON_INITIALISMS =
      Set.of(
          "ACL", "API", "ASCII", "CIDR", "CPU", "CSS", "DNS", "EOF", "FQDN", "GUID", "HTML", "HTTP",
          "HTTPS", "ID", "IP", "IPC", "IPv4", "IPv6", "JSON", "LHS", "PID", "QOS", "QPS", "RAM",
          "RHS", "RPC", "SLA", "SMTP", "SQL", "SSH", "TCP", "TLS", "TTL", "UDP", "UI", "UID",
          "UUID", "URI", "URL", "UTF8", "VM", "XML", "XMPP", "XSRF", "XSS");

  public static final Map<String, String> COMMON_EXCEPTIONS =
      Map.of(
          "oauth", "OAuth",
          "ipv4", "IPv4",
          "ipv6", "IPv6");

  private Initialisms() {}
}
