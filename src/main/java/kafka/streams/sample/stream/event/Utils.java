package kafka.streams.sample.stream.event;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import lombok.val;

public class Utils {

  public static final ZoneId UTC = ZoneId.of("UTC");
  public static final ZoneId ASIA_TOKYO = ZoneId.of("Asia/Tokyo");

  public static LocalDateTime getLocalDateTime(Instant time) {
    return time.atZone(ASIA_TOKYO).toLocalDateTime();
  }

  public static String getLocalDateTime(long timestamp) {
    val dt = getLocalDateTime(Instant.ofEpochMilli(timestamp));
    return dt.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
  }
}
