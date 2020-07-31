package kafka.streams.sample.stream.event;

import com.google.common.base.CaseFormat;

public enum Store {
  CHUNK_NUM_AGGREGATION,
  USER_ID_AGGREGATION;

  private final String storeName;

  Store() {
    this.storeName = CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_HYPHEN, this.name());
  }

  public String getName() {
    return this.storeName;
  }
}
