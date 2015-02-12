import java.awt.List;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Scanner;


public class BuildSentences {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String fileName=args[0];
		//String fileName="b3";
		int numwords=0;
		int maxwordlength=0;
		int n=0; //Input String length
		ArrayList<String> dict = new ArrayList<String>();
		String str = null;
		try{

		    //Create object of FileReader
			BufferedReader br = new BufferedReader(new FileReader(fileName));
			numwords=Integer.parseInt(br.readLine());
			//System.out.println("Numwords:"+numwords);
			for(int i=0;i<numwords;i++){
				dict.add(br.readLine());
				}
			
			str=br.readLine();
			n=str.length();
			br.close();
		   
		    }catch(Exception e){
		            System.out.println("Error while reading file line by line:" 
		            + e.getMessage());                      
		    }
		for(int i=0;i<numwords;i++){
			int wordlength=dict.get(i).length();
			if(wordlength>maxwordlength){
				maxwordlength=wordlength;
			}
		}
		//System.out.println(maxwordlength);
		int[][] DPtable=new int[n][maxwordlength];
		for(int i=0;i<n;i++){
			for(int j=0;j<maxwordlength;j++){
				if((n-i-j)>0){
				//System.out.println("i"+i+"j"+j);
				String substr=(String) str.subSequence(i,i+j+1);
				//System.out.println(substr);
				
				if(dict.contains(substr)){
					DPtable[i][j]=1;
				}
				
			}
				//System.out.print(DPtable[i][j]);
				}
			//System.out.println();
		}
		ArrayList<String> sentencewords=new ArrayList<String>();
		ArrayList<ArrayList<String>> sentences=new ArrayList<ArrayList<String>>();
		//sentencewords.add("str1");
		//sentences.add((ArrayList<String>) sentencewords.clone());
		//System.out.println(sentences);
		//sentencewords.add("str2");
		//System.out.println(sentences);
		getSentences(str,sentencewords, DPtable,0,sentences);
		int k=sentences.size();
		System.out.println(k);
		for(int i=0;i<k;i++){
			int senlen=sentences.get(i).size();
			for(int j=0;j<senlen;j++){
			System.out.print(sentences.get(i).get(j)+" ");
			}
			System.out.println();
		}
	}

	private static void getSentences(String str,
			ArrayList<String> sentencewords, int[][] dPtable, int i, ArrayList<ArrayList<String>> sentences) {
		//System.out.println(sentencewords);
		//System.out.println(dPtable[i].length);
		
		int maxlength=dPtable[i].length;
		for(int j=0;j<maxlength;j++){
			if(dPtable[i][j]==1){
				ArrayList<String> sen=(ArrayList<String>) sentencewords.clone();
				sen.add((String) str.subSequence(i, i+j+1));
				
				if((i+j+1)==str.length()){
					sentences.add(sen);
					//System.out.println(sen);
					break;
				}
				else {
					getSentences(str,sen, dPtable,i+j+1,sentences);
				}
			}
		}
		// TODO Auto-generated method stub
		
	}
	

}
