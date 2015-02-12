import java.util.Arrays;

public class Main{
   public static void main(String args[]){
     // Test Q1 implementation
     // you do not need to write this code. 
     // create and populate array A with values
    int B[] = {-2,4,-1,-5,2};
     int A[] = {42,2,9,25,49,0,123,45,2,5,6,18,34,58,76,42,33,33,23,27,42,8,4,100,10,94,62,57,62,34,15}; 
     // call PSort 
     PSort.parallelSort(A, 0, A.length);
     // verify if A is sorted
     System.out.println(Arrays.toString(A));
     // ... verification code --- written by us. 
     // (you do not need to write this code). 

     // Test Q2 implementation with 3 threads
    // int result = PSearch.parallelSearch(20, A, 3);
     // should return -1 as 20 is not in array
     // ... verification code --- written by us. 
     // (you do not need to write this code)
     System.exit(0);
   }
}
