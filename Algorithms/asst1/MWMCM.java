import java.io.*;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Scanner;


public class MWMCM {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String fileName=args[0]+".in";
		int numInstances=0;
		int numTics = 0;
		int numTacs = 0;
		int[][] ticsT = null;
		int[][] tacsT = null;
		ConvexBipartiteGraph[] cbgArray = null;
		try{

	    //Create object of FileReader
	    Scanner input = new Scanner(new File(fileName));
	    numInstances=input.nextInt();
	    cbgArray=new ConvexBipartiteGraph[numInstances];
	    
	    //Populate an array of Convex Bipartite Graphs by reading from the input file
	    for (int giter=0;giter<numInstances;giter++){
	    numTics=input.nextInt();
	    numTacs=input.nextInt();
	    cbgArray[giter]=new ConvexBipartiteGraph(numTics,numTacs);
	    ticsT=new int[numTics][4];
	    tacsT=new int[numTacs][2];
	    for (int i=0;i<numTics;i++){
	    	ticsT[i][0]=input.nextInt();
	    	ticsT[i][1]=input.nextInt();
	    	ticsT[i][2]=input.nextInt();
	    	ticsT[i][3]=input.nextInt();
	    }
	    for (int i=0;i<numTacs;i++){
	    	tacsT[i][0]=input.nextInt();
	    	tacsT[i][1]=input.nextInt();

	    }
	    cbgArray[giter].setTics(ticsT);
	    cbgArray[giter].setTacs(tacsT);
	    }
	    input.close();
	    
	    }catch(Exception e){
	            System.out.println("Error while reading file line by line:" 
	            + e.getMessage());                      
	    }
		
		//Opens a new file but writes nothing now. Later calls to the printwriter will append text
		PrintWriter writer;
		try {
			//new FileOutputStream(new File("file4.out"), true)
			writer = new PrintWriter(new FileOutputStream(new File(args[0]+".out")));
			
					writer.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		
		//Main loop: Iterates over all instances of CBGs to find all possible MWMCMs
		for(int giter=0;giter<numInstances;giter++){
		ConvexBipartiteGraph cbg=cbgArray[giter];
	    int[][] testEdgeSet=cbg.generateEdges();
		int numNodes=Math.max(cbg.tacs[cbg.numTacs-1][0],cbg.tics[cbg.numTics-1][0]);
		int numEdges=testEdgeSet.length;
		int numEdgeSets=(int)Math.pow(2,numEdges);
		int[] numEdgeSetsBinary=toBinaryArray(numEdgeSets,numEdges+1);
		//printArray(numEdgeSetsBinary);
		int maxcar=0;
		int maxweight=0;
		int mwmcmCount=0;
		ArrayList<int[][]> MWMCMs = new ArrayList<int[][]>();
		for (int i=1;i<numEdgeSets;i++){
			int[] iBinary=toBinaryArray(i,numEdges);
			int[] edgeSet=new int[sumArrayElements(iBinary)];
			int edgeIter=0;
			for (int j=0;j<iBinary.length;j++){
				if(iBinary[j]==1){
					edgeSet[edgeIter++]=j;
					}
			}
			int weight=isMatching(testEdgeSet,edgeSet,numNodes);
			if(weight==0)
				continue;
			
			int car=2*edgeSet.length;
			if(car<maxcar)
				continue;
			else if (car==maxcar){
				if(weight<maxweight)
					continue;
				else if(weight==maxweight){
					mwmcmCount++;
					int[][] edgeSetProper=getEdges(testEdgeSet,edgeSet);
					MWMCMs.add(edgeSetProper);
				}
				else {
					mwmcmCount=1;
					maxweight=weight;
					MWMCMs.clear();
					int[][] edgeSetProper=getEdges(testEdgeSet,edgeSet);
					MWMCMs.add(edgeSetProper);
				}
					
			}
			else {
				maxcar=car;
				mwmcmCount=1;
				maxweight=weight;
				MWMCMs.clear();
				int[][] edgeSetProper=getEdges(testEdgeSet,edgeSet);
				MWMCMs.add(edgeSetProper);
				}
		}
		sortMWMCM(MWMCMs);
		writeToFile(MWMCMs,args[0]);
		}
	}
	private static void sortMWMCM(
			ArrayList<int[][]> mWMCMs) {
		for(int i=0;i<mWMCMs.size();i++){
			
			java.util.Arrays.sort(mWMCMs.get(i), new java.util.Comparator<int[]>() {
			    public int compare(int[] a, int[] b) {
			        return Integer.compare(a[1], b[1]);
			    }
			});
			
			
			
		}
		//Sort the graphs
		Collections.sort(mWMCMs, new java.util.Comparator<int[][]>() {
		    public int compare(int[][] a, int[][] b) {
		        for(int i=0;i<a.length;i++){
		        	int uct=Integer.compare(a[i][2], b[i][2]);
		        	int uc=0;
		        	if(uct==0)
		        		uc=Integer.compare(a[i][0], b[i][0]);
		        	else uc=uct;
		        	int vc=Integer.compare(a[i][1], b[i][1]);
		        	
		        	if(vc!=0)
		        		return vc;
		        	else if (uc!=0)
		        		return uc;
		        	/*int uc=Integer.compare(a[i][0], b[i][0]);
		        	int vc=Integer.compare(a[i][1], b[i][1]);
		        	
		        	if(uc!=0)
		        		return uc;
		        	else if (vc!=0)
		        		return vc;*/
		        }
		        return 0;
		    }
		});
		// TODO Auto-generated method stub
		
	}
	private static void writeToFile(ArrayList<int[][]> mWMCMs, String file) {
		// TODO Auto-generated method stub
		PrintWriter writer;
		try {
			writer = new PrintWriter(new FileOutputStream(new File(file+".out"), true));
			writer.print(mWMCMs.size());
			writer.println();
			for (int i=0;i<mWMCMs.size();i++){
				int numEdges=mWMCMs.get(i).length;
				for(int j=0;j<numEdges;j++){
					writer.print(mWMCMs.get(i)[j][0]);
					writer.print(":");
					writer.print(mWMCMs.get(i)[j][1]);
					writer.print(" ");
					}
				writer.println();
			}
					writer.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	private static int[][] getEdges(int[][] testEdgeSet, int[] edgeSet) {
		// TODO Auto-generated method stub
		int[][] edgeSetProper=new int[edgeSet.length][3];
		for(int i=0;i<edgeSet.length;i++){
			edgeSetProper[i][0]=testEdgeSet[edgeSet[i]][0];
			edgeSetProper[i][1]=testEdgeSet[edgeSet[i]][1];
			edgeSetProper[i][2]=testEdgeSet[edgeSet[i]][3];
			}
		return edgeSetProper;
	}
	public static int isMatching(int[][] testEdgeSet,int[] edgeSet, int numNodes){
		int weight=0;
		int[] nodesU=new int[numNodes+1];
		int[] nodesV=new int[numNodes+1];
		
		for(int i=0;i<edgeSet.length;i++){
			if(nodesU[testEdgeSet[edgeSet[i]][0]]==1){
					return 0;}
			else {nodesU[testEdgeSet[edgeSet[i]][0]]=1;}
				
			if(nodesV[testEdgeSet[edgeSet[i]][1]]==1){
				return 0;}
			else {nodesV[testEdgeSet[edgeSet[i]][1]]=1;}
			weight+=testEdgeSet[edgeSet[i]][2];
		}
		return weight;
		
	}
	public static int sumArrayElements(int[] toSumArray){
		int sum=0;
		for(int i=0;i<toSumArray.length;i++){
			sum=sum+toSumArray[i];
		}
		return sum;
	}
	
	public static void printArray(int[] binaryArray){
		for(int i=0;i<binaryArray.length;i++){
			System.out.print(binaryArray[i]);
		}
		//Go to a new line
		System.out.println("");
	}
	public static int[] toBinaryArray(int num, int binaryLength){
		String tempstr=Integer.toBinaryString(num);
		char[] tempchararr=tempstr.toCharArray();
		int numLength=tempchararr.length;
		int[] binaryArray=new int[binaryLength];
		for(int i=0;i<binaryLength-numLength;i++){
			binaryArray[i]=0;
		}
		for(int i=0;i<numLength;i++){
		binaryArray[binaryLength-numLength+i]=Character.getNumericValue(tempchararr[i]);
		}
		return binaryArray;
		
	}
}
