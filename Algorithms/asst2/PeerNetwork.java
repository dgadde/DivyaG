import java.io.File;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Scanner;


public class PeerNetwork {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		List<List<int[]>> adj=new ArrayList<List<int[]>>();
		String fileName=args[0]+".in";
		//String fileName="in2.txt";
		int numNodes=0;
		int numTraces=0;
		int startNode = 0;
		int endNode=0;
		int startTime=0;
		int endTime=0;
		int[][] traces = null;
		try{

		    //Create object of FileReader
		    Scanner input = new Scanner(new File(fileName));
		    numNodes=input.nextInt();
		    numTraces=input.nextInt();
		    traces=new int[numTraces][3];
		    
		    for(int i=0;i<numTraces;i++){
		    	int node1=input.nextInt();
		    	int node2=input.nextInt();
		    	int time=input.nextInt();
		    	traces[i][0]=node1;
		    	traces[i][1]=node2;
		    	traces[i][2]=time;
		    	}
		    //printAdj(adj);
		    startNode=input.nextInt();
		    endNode=input.nextInt();
		    startTime=input.nextInt();
		    endTime=input.nextInt();
		    input.close();
		    
		    }catch(Exception e){
		            System.out.println("Error while reading file line by line:" 
		            + e.getMessage());                      
		    }
		
		for (int i = 0; i < numNodes; i++) {
			adj.add(new ArrayList<int[]>());
			}
		
		for(int i=0;i<numTraces;i++){
			int node1=traces[i][0];
			int node2=traces[i][1];
			int time=traces[i][2];
			if((time<startTime) || (time>endTime)){
				continue;
			}
			else {
				adj.get(node1).add(new int[]{node2,time});
				adj.get(node2).add(new int[]{node1,time});
				}
			
		}
		List<List<int[]>> levels=BFS(adj, startNode, endNode, startTime);
		//printAdj(levels);
		List<int[]> path=getPath(startNode, endNode, 0,startTime,levels,adj);
		if(path!=null)
			printPath(path);
		else System.out.println(0);
	}


	private static void printPath(List<int[]> path) {
		// TODO Auto-generated method stub
		System.out.println(path.size()-1);
		for(int i=1;i<path.size();i++){
			int node1=path.get(i-1)[0];
			int node2=path.get(i)[0];
			int time=path.get(i)[1];
			if (node2<node1){
				int temp=node1;
				node1=node2;
				node2=temp;
			}
				
			System.out.println(node1+" "+node2+" "+time);}
		
	}


	private static List<int[]> getPath(int u, int v, int lu,int time,
			List<List<int[]>> levels, List<List<int[]>> adj) {
		if(lu==levels.size()-2){
			int nexttime=isEdge(u,v,time,adj);
			if(nexttime!=-1){
				List<int[]> temppath=new ArrayList<int[]>();
				temppath.add(new int[]{u,time});
				temppath.add(new int[]{v,nexttime});
				return temppath;
			}
			else return null;
		}
		else {
			for(int i=0;i<levels.get(lu+1).size();i++){
				int nexttime=isEdge(u,levels.get(lu+1).get(i)[0],time,adj);
				if(nexttime!=-1){
					List<int[]> temppath=getPath(levels.get(lu+1).get(i)[0],v,lu+1,levels.get(lu+1).get(i)[1], levels,adj);
					
					if((temppath!=null)){
						List<int[]> temppath1=new ArrayList<int[]>();
						temppath1.add(new int[]{u,time});
						temppath1.addAll(temppath);
						return temppath1;
					}
						
				}
				
				
				}
			
		}
		// TODO Auto-generated method stub
		return null;
	}


	private static int isEdge(int u, int v,int time1, List<List<int[]>> adj) {
		// TODO Auto-generated method stub
		for(int i=0;i<adj.get(u).size();i++){
			int nexttime1=adj.get(u).get(i)[1];
			if((adj.get(u).get(i)[0]==v) &&(nexttime1>=time1))
				return nexttime1;
			
		}
			
		return -1;
	}


	@SuppressWarnings("unchecked")
	private static List<List<int[]>> BFS(List<List<int[]>> adj, int root, int target, int startTime) {
		// TODO Auto-generated method stub
		List<List<int[]>> levels=new ArrayList<List<int[]>>();
		levels.add(new ArrayList<int[]>());
		int currlvl=1;
		int nxtlvl=0;
		Queue queue = new LinkedList();
		Queue queuetimes = new LinkedList();
		queue.add(root);
		queuetimes.add(adj.get(root).get(0)[1]);
		int lvlnum=0;
		levels.get(lvlnum).add(new int[]{root,adj.get(root).get(0)[1]});
		while(currlvl!=0){
			int currnode=(int) queue.poll();
			currlvl--;
			int currtime=(int) queuetimes.poll();
			if((currnode==target)){
				break;}
			for(int i=0;i<adj.get(currnode).size();i++){
				int toNode=adj.get(currnode).get(i)[0];
				int temptime=adj.get(currnode).get(i)[1];
				if(hasNode(levels,toNode,temptime)||temptime<currtime)
					continue;
				else {
					queue.add(toNode);
					queuetimes.add(temptime);
					nxtlvl++;
				}
					
				}
			if(currlvl==0){
				
				currlvl=nxtlvl;
				nxtlvl=0;
				lvlnum++;
				levels.add(new ArrayList<int[]>());
				Queue queued = new LinkedList(queue);
				Queue queuetimesd = new LinkedList(queuetimes);
				int size=queued.size();
				for(int k=0;k<size;k++){
					int node=(int) queued.poll();
					int time=(int) queuetimesd.poll();
					levels.get(lvlnum).add(new int[]{node,time});
					if((node==target)){
					   break;}
				}
				}
			}
		return levels;
		}
		
	private static boolean hasNode(List<List<int[]>> levels, int toNode, int toTime) {
		// TODO Auto-generated method stub
		if (toNode==levels.get(0).get(0)[0])
			return true;
		for(int i=0;i<levels.size();i++)
			for(int j=0;j<levels.get(i).size();j++)
				if((levels.get(i).get(j)[0]==toNode)&&(levels.get(i).get(j)[1]==toTime))
					return true;
				
		return false;
	}

	private static void printAdj(List<List<int[]>> adj) {
		// TODO Auto-generated method stub
		for(int i=0;i<adj.size();i++){
			System.out.println();
			for(int j=0;j<adj.get(i).size();j++){
				System.out.print("("+adj.get(i).get(j)[0]+","+adj.get(i).get(j)[1]+")");
			}
				
		}
		
	}

}
