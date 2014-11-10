package util;

import java.util.ArrayList;
import java.util.List;

public class UtilMisc {

	public static <T> T getRandomElement(List<T> list) {
		if (list == null || list.size() == 0) {
			return null;
		}

		return list.get((int) (Math.random() * list.size()));
	}

	public static <T> T removeRandomElement(List<T> list) {
		if (list == null || list.size() == 0) {
			return null;
		}

		return list.remove((int) (Math.random() * list.size()));
	}

	/** 
	 * Similar to List.removeAll() but considers the number of occurrences
	 */
	public static <T extends Comparable<T>> List<T> getDifferenceList(List<T> list, List<T> listToRemove) {
		List<T> result = new ArrayList<>();
		List<T> tmpToRemove = new ArrayList<>(listToRemove);
		for (T element : list) {
			if (tmpToRemove.contains(element)) {
				tmpToRemove.remove(element);
			} else {
				result.add(element);
			}
		}
		return result;
	}
}
