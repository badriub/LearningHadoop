package bm.hadoop.pagerank;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.lang.StringUtils;

public class Node {
	public static int INFINITE = Integer.MAX_VALUE;
	public static final char fieldSeparator = '\t';

	private double pageRank = 0.25;
	private String[] adjacentNodeNames;

	public boolean containsAdjacentNodes() {
		return adjacentNodeNames != null;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(pageRank).append(fieldSeparator);

		if (getAdjacentNodeNames() != null) {
			sb.append(fieldSeparator).append(
					StringUtils.join(getAdjacentNodeNames(), fieldSeparator));
		}
		return sb.toString();
	}

	public static Node fromMR(String value) throws IOException {
		String[] parts = StringUtils.splitPreserveAllTokens(value,
				fieldSeparator);
		if (parts.length < 2) {
			throw new IOException("Expected 2 or more parts but received "
					+ parts.length);
		}
		Node node = new Node();
		node.setPageRank(Integer.valueOf(parts[0]));
		if (parts.length > 2) {
			node.setAdjacentNodeNames(Arrays
					.copyOfRange(parts, 2, parts.length));
		}
		return node;
	}

	public double getPageRank() {
		return pageRank;
	}

	public void setPageRank(double pageRank) {
		this.pageRank = pageRank;
	}

	public String[] getAdjacentNodeNames() {
		return adjacentNodeNames;
	}

	public Node setAdjacentNodeNames(String[] adjacentNodeNames) {
		this.adjacentNodeNames = adjacentNodeNames;
		return this;
	}

}
