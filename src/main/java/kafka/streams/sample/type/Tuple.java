package kafka.streams.sample.type;

import lombok.RequiredArgsConstructor;
import lombok.Value;

@Value
@RequiredArgsConstructor(staticName = "of")
public class Tuple<L, R> {
  private final L left;
  private final R right;
}
