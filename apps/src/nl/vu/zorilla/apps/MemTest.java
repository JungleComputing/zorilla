package nl.vu.zorilla.apps;

public class MemTest {


	public static void main(String[] args) {
		int size = Integer.parseInt(args[0]);

		byte[][] array = new byte[size][];

		for (int i = 0; i < size; i++) {
			array[i] = new byte[1024*1024];
		}

	}
}

