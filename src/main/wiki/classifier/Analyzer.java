package wiki.classifier;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Analyzer {

	public static void main(String[] args) throws FileNotFoundException, IOException {

		String filepath = args[0];
		String filepath2 = args[1];

		Integer[] counter10 = new Integer[] { 0, 0, 0, 0, 0, 0, 0 };
		Integer[] counter20 = new Integer[] { 0, 0, 0, 0, 0, 0, 0 };

		List<String> title500List = new ArrayList<>();
		List<String> title1000List = new ArrayList<>();

		int numberOfLines = 0;
		try (BufferedReader br = new BufferedReader(new FileReader(filepath))) {
			try (BufferedWriter bw = new BufferedWriter(new FileWriter(filepath2))) {
				String line;
				while ((line = br.readLine()) != null) {
					String[] lineArray = line.split(",");

					double wordsTotal = Integer.parseInt(lineArray[1].trim());
					double words500 = Integer.parseInt(lineArray[2].trim());
					double words1000 = words500 + Integer.parseInt(lineArray[3].trim());
					double words2000 = words1000 + Integer.parseInt(lineArray[4].trim());
					double words3000 = words2000 + Integer.parseInt(lineArray[5].trim());
					double words4000 = words3000 + Integer.parseInt(lineArray[6].trim());
					double words5000 = words4000 + Integer.parseInt(lineArray[7].trim());

					if (wordsTotal < 50) {
						continue;
					}

					if (words500 / wordsTotal > 0.9) {
						counter10[0]++;
						title500List.add(lineArray[0]);
					}
					if (words500 / wordsTotal > 0.8) {
						counter20[0]++;
					}

					if (words1000 / wordsTotal > 0.9) {
						counter10[1]++;
						title1000List.add(lineArray[0]);
					}
					if (words1000 / wordsTotal > 0.8) {
						counter20[1]++;
					}

					if (words2000 / wordsTotal > 0.9) {
						counter10[2]++;
					}
					if (words2000 / wordsTotal > 0.8) {
						counter20[2]++;
					}

					if (words3000 / wordsTotal > 0.9) {
						counter10[3]++;
					}
					if (words3000 / wordsTotal > 0.8) {
						counter20[3]++;
					}

					if (words4000 / wordsTotal > 0.9) {
						counter10[4]++;
					}
					if (words4000 / wordsTotal > 0.8) {
						counter20[4]++;
					}

					if (words5000 / wordsTotal > 0.9) {
						counter10[5]++;
					}
					if (words5000 / wordsTotal > 0.8) {
						counter20[5]++;
					}

					bw.write(line);
					numberOfLines++;

					if (numberOfLines % 100000 == 0) {
						System.out.println("10%: " + Arrays.deepToString(counter10));
						System.out.println("20%: " + Arrays.deepToString(counter20));
					}
				}
			}
		}
		System.out.println("--- final ---------------------------");
		System.out.println("10%: " + Arrays.deepToString(counter10));
		System.out.println("20%: " + Arrays.deepToString(counter20));

		System.out.println("500 (90%) " + Arrays.deepToString(title500List.toArray()));
		System.out.println("1000 (90%) " + Arrays.deepToString(title1000List.toArray()));
	}
}
