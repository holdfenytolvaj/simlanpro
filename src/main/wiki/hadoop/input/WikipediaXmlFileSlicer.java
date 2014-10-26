package wiki.hadoop.input;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.io.OutputBuffer;

/** Extract a page from the xml file */
public class WikipediaXmlFileSlicer {

	private static final byte[] endPagePattern, startPagePattern;
	static {
		try {
			startPagePattern = "<page>".getBytes("UTF-8");
			endPagePattern = "</page>".getBytes("UTF-8");
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
	}

	private static long getNextEndPagePatternPosition(InputStream in) throws IOException {
		long movedPos;
		if ((movedPos = getNextStartPagePatternPosition(in)) == -1) {
			return -1;
		}
		int matchingEndPos = 0;
		int data;
		while ((data = in.read()) != -1) {
			movedPos++;
			if (endPagePattern[matchingEndPos] == data) {
				if (matchingEndPos == endPagePattern.length - 1) {
					return movedPos;
				}
				matchingEndPos++;
			} else {
				matchingEndPos = 0;
			}
		}
		return -1;
	}

	private static long getNextStartPagePatternPosition(InputStream in) throws IOException {
		int data;
		int matchingStartPos = 0;
		long movedPos = 0;
		while ((data = in.read()) != -1) {
			movedPos++;
			if (startPagePattern[matchingStartPos] == data) {
				if (matchingStartPos == startPagePattern.length - 1) {
					return movedPos;
				}
				matchingStartPos++;
			} else {
				matchingStartPos = 0;
			}

		}
		return -1;
	}

	public static long getNextPageContent(InputStream in, OutputBuffer out) throws IOException {
		long movedPos;
		if ((movedPos = getNextStartPagePatternPosition(in)) == -1) {
			return -1;
		}
		if (out != null) {
			out.write(startPagePattern);
		}
		int matchingEndPos = 0;
		int data;
		while ((data = in.read()) != -1) {
			movedPos++;
			if (out != null) {
				out.write(data);
			}
			if (endPagePattern[matchingEndPos] == data) {
				if (matchingEndPos == endPagePattern.length - 1) {
					return movedPos;
				}
				matchingEndPos++;
			} else {
				matchingEndPos = 0;
			}
		}
		return -1;
	}

	public static long getEndPositionForSplit(InputStream inputStream, int numberOfWikipagePerFilesplit) throws IOException {
		long length = -1;
		for (int i = 0; i < numberOfWikipagePerFilesplit; i++) {
			long l = getNextEndPagePatternPosition(inputStream);
			if (l == -1) {
				return length;
			} else {
				length += l;
			}
		}
		return length;
	}
}
