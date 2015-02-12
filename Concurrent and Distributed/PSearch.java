import java.util.ArrayList;
import java.util.concurrent.*;

public class PSearch implements Callable<Integer>{
	int x;
	int[] A;
	int start;
	int end;
	public static ExecutorService threadpool=Executors.newCachedThreadPool();
	
	
	public PSearch(int x2, int[] a2, int start, int end) {
		x=x2;
		A=a2;
		this.start=start;
		this.end=end;
		// TODO Auto-generated constructor stub
	}
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		int A[] = {20,42,2,9,25,49,0,123,45,2,5,6,18,34,58,76,42,33,33,23,27,42,8,4,100,10,94,62,57,62,34,15,54,34,32,54,12,111,113,53,8,9,3,14,15,26,23,25,21,78,6,87,777,75,33,66,88};;
		int x=123;
		
		System.out.println(PSearch.parallelSearch(x, A, 4));
		

	}
	public static int parallelSearch(int x,int[] A, int numThreads){
		int size=A.length;
		int subArraySize=size/numThreads;
		ArrayList<Future<Integer>> f=new ArrayList<Future<Integer>>(); 
		for(int i=0;i<numThreads;i++){
			int start=i*subArraySize;;
			int end;
			if(i==(numThreads-1)){
				end=size;
			}
			else {			
				end=(i+1)*subArraySize;				
			}
			//System.out.println("Start and End"+start+" ,"+end);
			f.add(threadpool.submit(new PSearch(x,A,start,end)));			
		}
		for(int i=0;i<numThreads;i++){
			int val=-1;
			try {
				val = f.get(i).get();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				} catch (ExecutionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if(val!=-1){
				return val;
			}
		}
		
		return -1;
	}

	@Override
	public Integer call()  {
		// TODO Auto-generated method stub
		for(int i=start;i<end;i++){
			if(x==A[i]){
				return i;
			}
		}
		return -1;
	}

}
