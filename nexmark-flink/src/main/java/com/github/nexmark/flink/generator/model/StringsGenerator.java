package com.github.nexmark.flink.generator.model;

import java.util.Random;

/** Generates strings which are used for different field in other model objects. */
public class StringsGenerator {

  /** Smallest random string size. */
  private static final int MIN_STRING_LENGTH = 3;

  /** Return a random string of up to {@code maxLength}. */
  public static String nextString(Random random, int maxLength) {
    return nextString(random, maxLength, ' ');
  }

  public static String nextString(Random random, int maxLength, char special) {
    int len = MIN_STRING_LENGTH + random.nextInt(maxLength - MIN_STRING_LENGTH);
    StringBuilder sb = new StringBuilder();

    while (len-- > 0) {
      // Unsure if this was just arbitrarily chosen, if the random int happens to be 0
      // then this option is selected over the else case
      if (random.nextInt(13) == 0) {
        sb.append(special);
      } else {
        sb.append((char) ('a' + random.nextInt(26)));
      }
    }

    // trim() - removes trailing and/or leading whitespace
    return sb.toString().trim();
  }

  /** Return a random string of exactly {@code length}. */
  public static String nextExactString(Random random, int length) {
    StringBuilder sb = new StringBuilder();
    int rnd = 0;
    int n = 0; // number of random characters left in rnd

    while (length-- > 0) {
      if (n == 0) {
        rnd = random.nextInt();
        n = 6; // log_26(2^31)
      }

      sb.append((char) ('a' + rnd % 26));
      rnd /= 26;
      n--;
    }

    return sb.toString();
  }

  /**
   * Return a random {@code string} such that {@code currentSize + string.length()} is on average
   * {@code averageSize}.
   */
  public static String nextExtra(Random random, int currentSize, int desiredAverageSize) {
    // Think this is trying to lower to desiredAverageSize if currentSize has been greater than it
    if (currentSize > desiredAverageSize) {
      return "";
    }

    desiredAverageSize -= currentSize;
    int delta = (int) Math.round(desiredAverageSize * 0.2);
    int minSize = desiredAverageSize - delta;
    int desiredSize = minSize + (delta == 0 ? 0 : random.nextInt(2 * delta));

    // Return a random string of exactly {@code length}.
    return nextExactString(random, desiredSize);
  }
}
