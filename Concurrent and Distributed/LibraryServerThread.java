import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Scanner;


public class LibraryServerThread extends Thread {
	Socket theClient;
	LibraryTable table;

	public LibraryServerThread(Socket accept, LibraryTable table) {
		// TODO Auto-generated constructor stub
		this.table=table;
		theClient=accept;
	}

	

	@Override
	public void run() {
		// TODO Auto-generated method stub
		Scanner sc = null ;
		try {
		sc = new Scanner ( theClient . getInputStream ( ) ) ;
		PrintWriter pout = new PrintWriter ( theClient . getOutputStream ( ) ) ; 
		String command = sc . nextLine ();
		Scanner st = new Scanner(command);
		String clientid = st.next();
		String booknum=st.next();
		String reqret=st.next();
		
		if(reqret.equals("reserve")){
			if(table.request(booknum, clientid)){
				pout.println(clientid +" "+ booknum);
			}
			else {
				pout.println("fail "+ clientid +" "+ booknum);
			}
		}
		else if(reqret.equals("return")){
			if(table.returnBook(booknum, clientid)){
				pout.println("free "+clientid +" "+ booknum);
			}
			else {
				pout.println("fail "+ clientid +" "+ booknum);
			}
		}
		pout.flush();
		theClient.close();
		}catch( IOException e ) { e.printStackTrace(); ;
		} finally {
		sc . close ();
		}
		}

}
