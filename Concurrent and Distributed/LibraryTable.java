import java.util.HashMap;


public class LibraryTable {
	int numbooks;
	HashMap<String, String> ledger;
	public LibraryTable(int numbooks){
		this.numbooks=numbooks;
	
		ledger=new HashMap<String, String>();
		for(int i=1;i<=numbooks;i++){
			String key="b"+i;
			ledger.put(key, null);
			
		}
	}
	public synchronized boolean request(String book, String clientid){
		if(!ledger.containsKey(book)){
			return false;
		}
		if(ledger.get(book)!=null){
			return false;
		}
		else {
			ledger.put(book, clientid);
			return true;
		}
	}
	public synchronized boolean returnBook(String book, String clientid){
		if(!ledger.containsKey(book)){
			return false;
		}
		if(clientid.equals(ledger.get(book))){
			ledger.put(book, null);
			return true;
		}
		else {
			return false;
		}
	}
	

}
