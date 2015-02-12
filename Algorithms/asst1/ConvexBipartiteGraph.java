import java.util.*;
public class ConvexBipartiteGraph {
	 int numTics;
	 int numTacs;
	 int[][] tics=new int[numTics][4];
	 int[][] tacs=new int[numTacs][2];
 public ConvexBipartiteGraph(int num1, int num2){
	 numTics=num1;
	 numTacs=num2;
 }
 public void setTics(int[][] ticsT){
	 tics=ticsT;
	 //printTics();
	 
 }
 
 public void setTacs(int[][] tacsT){
	 tacs=tacsT;
 }
 public void printTics(){
	 for(int i=0;i<tics.length;i++)
		 for(int j=0;j<4;j++)
			 System.out.println(tics[i][j]);
 }
 public int[][] generateEdges(){
	 
	 ArrayList<Integer[]> edgeList = new ArrayList<Integer[]>();
	 for (int i=0;i<numTics;i++){
		 int min=tics[i][1];
		 int max=tics[i][2];
		 int wei=tics[i][3];
		 for(int j=min;j<=max;j++){
			 if(getTacIndex(j)>=0){
				Integer[] temp={tics[i][0],j,wei+tacs[getTacIndex(j)][1],max};
				edgeList.add(temp);
			 }
				 
		 }
	 }
	 int[][] edgeListI=new int[edgeList.size()][4];
	 for(int i=0;i<edgeList.size();i++){
		 edgeListI[i][0]=edgeList.get(i)[0];
		 edgeListI[i][1]=edgeList.get(i)[1];
		 edgeListI[i][2]=edgeList.get(i)[2];
		 edgeListI[i][3]=edgeList.get(i)[3];
				 }
	return edgeListI;
	  
 }
private int getTacIndex(int j) {
	// TODO Auto-generated method stub
	for (int i=0;i<numTacs;i++){
		if(tacs[i][0]==j)
			return i;
	}
	return -1;
}
}
