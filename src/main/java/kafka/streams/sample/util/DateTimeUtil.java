package kafka.streams.sample.util;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import lombok.val;
import org.apache.kafka.streams.kstream.Window;

public class DateTimeUtil {

  private static ZoneId ASIA_TOKYO = ZoneId.of("Asia/Tokyo");

  public static LocalDateTime getLocalDateTime(Instant instant) {
    return instant.atZone(ASIA_TOKYO).toLocalDateTime();
  }

  public static String getLocalDateTime(long timestamp) {
    val dt = getLocalDateTime(Instant.ofEpochMilli(timestamp));
    return dt.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
  }

  public static String getWindowStartAndEnd(Window window) {
    val localStartTime = getLocalDateTime(window.startTime());
    val startTime = localStartTime.format(DateTimeFormatter.ISO_LOCAL_TIME);
    val localEndTime = getLocalDateTime(window.endTime());
    val endTime = localEndTime.format(DateTimeFormatter.ISO_LOCAL_TIME);
    return startTime + " <==> " + endTime;
  }
}
