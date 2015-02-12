public class PSort implements Runnable{
	private int low, high;
	private int[] array;
	private static int count = 0;

	public PSort(int[] arr, int l, int h) {
		low = l;
		high = h;
		array = arr;
	}

	public void run() {
		PSort.parallelSort(array, low, high);
	}

	public static void parallelSort(int[] A, int begin, int end) {
		// Use last index of the array as pivot
		if(end - begin <= 1) return;
		int pivot = A[end-1];
		int finalpos = begin;
		for(int i = begin ; i < end-1 ; i++) {
			if(A[i] < pivot) {
				int temp = A[i];
				A[i] = A[finalpos];
				A[finalpos] = temp;
				finalpos++;
			}
		}
		int temp = A[finalpos];
		A[finalpos] = pivot;
		A[end-1] = temp;

		PSort sort1 = new PSort(A, begin, finalpos);
		Thread bottom = new Thread(sort1);
		bottom.start();

		PSort sort2 = new PSort(A, finalpos+1, end);
		Thread top = new Thread(sort2);
		top.start();

		try {
			bottom.join();
			top.join();
		}catch (InterruptedException e){}

	}
}

class PSearch {
	public static int parallelSearch(int x, int[] A, int numThreads) {
		// your implementation goes here.
		return 0;
	}
}