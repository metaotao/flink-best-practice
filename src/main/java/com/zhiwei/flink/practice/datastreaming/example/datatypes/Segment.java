package com.zhiwei.flink.practice.datastreaming.example.datatypes;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

/**
 * A Segment contains data about a continuous stretch of driving.
 */
public class Segment {
	public Long startTime;
	public int length;
	public int maxSpeed;
	public float erraticness;

	public Segment() {
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(startTime).append(",")
				.append(length).append(" events,")
				.append(maxSpeed).append(" kph,")
				.append(erraticnessDesc());

		return sb.toString();
	}

	public String erraticnessDesc() {
		switch ((int) (erraticness / 2.5)) {
			case 0:
				return "calm";
			case 1:
				return "busy";
			default:
				return "crazy";
		}
	}

	@Override
	public boolean equals(Object other) {
		return other instanceof Segment &&
				this.startTime == ((Segment) other).startTime;
	}

	@Override
	public int hashCode() {
		return (int) this.startTime.hashCode();
	}

	protected static float maxSpeed(ArrayList<ConnectedCarEvent> events) {
		ConnectedCarEvent fastest = Collections.max(events, new CompareBySpeed());
		return fastest.speed;
	}

	protected static long minTimestamp(ArrayList<ConnectedCarEvent> events) {
		ConnectedCarEvent first = Collections.min(events, new CompareByTimestamp());
		return first.timestamp;
	}

	protected static float stddevThrottle(ArrayList<ConnectedCarEvent> array) {
		float sum = 0.0f;
		float mean;
		float sum_of_sq_diffs = 0;

		for (ConnectedCarEvent event : array) {
			sum += event.throttle;
		}

		mean = sum / array.size();
		for (ConnectedCarEvent event : array) {
			sum_of_sq_diffs += (event.throttle - mean) * (event.throttle - mean);
		}

		return (float) Math.sqrt(sum_of_sq_diffs / array.size());
	}

	private static class CompareBySpeed implements Comparator<ConnectedCarEvent> {
		public int compare(ConnectedCarEvent a, ConnectedCarEvent b) {
			return Float.compare(a.speed, b.speed);
		}
	}

	private static class CompareByTimestamp implements Comparator<ConnectedCarEvent> {

		@Override
		public int compare(ConnectedCarEvent a, ConnectedCarEvent b) {
			return Long.compare(a.timestamp, b.timestamp);
		}
	}
}
