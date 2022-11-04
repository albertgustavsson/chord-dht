package se.umu.cs.ads.chord;

import java.math.BigInteger;

public class RangeUtils {
	/**
	 * Check if a value is in the given range. The comparisons are done modulo {@code mod}.
	 * For a {@code mod} of 10, the value space is [0,9].
	 * For that space, a test for the range (8,1) would be true for the values 9 and 0.
	 * @param value the value to check.
	 * @param rangeStart the start of the range (exclusive).
	 * @param rangeEnd the end of the range (exclusive).
	 * @param mod the modulo (size of the value space).
	 * @return whether {@code value} is in the range or not.
	 */
	public static boolean valueIsInRangeExclExcl(BigInteger value, BigInteger rangeStart, BigInteger rangeEnd, BigInteger mod) {
		// Intuitive non-circular way: value.compareTo(rangeStart) > 0 && value.compareTo(rangeEnd) < 0
		BigInteger valueMinusStart = value.subtract(rangeStart).mod(mod);
		BigInteger endMinusStart = rangeEnd.subtract(rangeStart).mod(mod);
		return valueMinusStart.compareTo(BigInteger.ZERO) > 0 && valueMinusStart.compareTo(endMinusStart) < 0;
	}

	/**
	 * Check if a value is in the given range. The comparisons are done modulo {@code mod}.
	 * For a {@code mod} of 10, the value space is [0,9].
	 * For that space, a test for the range (8,1] would be true for the values 9, 0 and 1.
	 * @param value the value to check.
	 * @param rangeStart the start of the range (exclusive).
	 * @param rangeEnd the end of the range (inclusive).
	 * @param mod the modulo (size of the value space).
	 * @return whether {@code value} is in the range or not.
	 */
	public static boolean valueIsInRangeExclIncl(BigInteger value, BigInteger rangeStart, BigInteger rangeEnd, BigInteger mod) {
		// Intuitive non-circular way: value.compareTo(rangeStart) > 0 && value.compareTo(rangeEnd) <= 0
		BigInteger valueMinusStart = value.subtract(rangeStart).mod(mod);
		BigInteger endMinusStart = rangeEnd.subtract(rangeStart).mod(mod);
		return valueMinusStart.compareTo(BigInteger.ZERO) > 0 && valueMinusStart.compareTo(endMinusStart) <= 0;
	}

	/**
	 * Check if a value is in the given range. The comparisons are done modulo {@code mod}.
	 * For a {@code mod} of 10, the value space is [0,9].
	 * For that space, a test for the range [8,1) would be true for the values 8, 9 and 0.
	 * @param value the value to check.
	 * @param rangeStart the start of the range (inclusive).
	 * @param rangeEnd the end of the range (exclusive).
	 * @param mod the modulo (size of the value space).
	 * @return whether {@code value} is in the range or not.
	 */
	public static boolean valueIsInRangeInclExcl(BigInteger value, BigInteger rangeStart, BigInteger rangeEnd, BigInteger mod) {
		// Intuitive non-circular way: value.compareTo(rangeStart) > 0 && value.compareTo(rangeEnd) <= 0
		BigInteger valueMinusStart = value.subtract(rangeStart).mod(mod);
		BigInteger endMinusStart = rangeEnd.subtract(rangeStart).mod(mod);
		return valueMinusStart.compareTo(BigInteger.ZERO) >= 0 && valueMinusStart.compareTo(endMinusStart) < 0;
	}

	/**
	 * Check if a value is in the given range. The comparisons are done modulo {@code mod}.
	 * For a {@code mod} of 10, the value space is [0,9].
	 * For that space, a test for the range [8,1] would be true for the values 8, 9, 0 and 1.
	 * @param value the value to check.
	 * @param rangeStart the start of the range (inclusive).
	 * @param rangeEnd the end of the range (inclusive).
	 * @param mod the modulo (size of the value space).
	 * @return whether {@code value} is in the range or not.
	 */
	public static boolean valueIsInRangeInclIncl(BigInteger value, BigInteger rangeStart, BigInteger rangeEnd, BigInteger mod) {
		// Intuitive non-circular way: value.compareTo(rangeStart) > 0 && value.compareTo(rangeEnd) <= 0
		BigInteger valueMinusStart = value.subtract(rangeStart).mod(mod);
		BigInteger endMinusStart = rangeEnd.subtract(rangeStart).mod(mod);
		return valueMinusStart.compareTo(BigInteger.ZERO) >= 0 && valueMinusStart.compareTo(endMinusStart) <= 0;
	}
}
