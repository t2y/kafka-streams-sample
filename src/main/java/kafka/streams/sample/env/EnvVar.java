package kafka.streams.sample.env;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum EnvVar {
  USER_NUMS(System.getenv().getOrDefault("USER_NUMS", "0")),
  USER_ID(System.getenv().getOrDefault("USER_ID", "0")),
  USER_NAME(System.getenv().getOrDefault("USER_NAME", "user0")),
  USER_EMAIL(System.getenv().getOrDefault("USER_EMAIL", "user0@example.com")),

  WITH_ASYNC(System.getenv().getOrDefault("WITH_ASYNC", "false"));

  private final String value;

  public Boolean getBoolValue() {
    return Boolean.valueOf(this.value);
  }

  public Long getLongValue() {
    return Long.valueOf(this.value);
  }
}
