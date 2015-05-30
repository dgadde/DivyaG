import java.io.IOException;
import java.net.ServerSocket;


public class TCPThread extends Thread {
	int port;
	LibraryTable table;
	public TCPThread(int port, LibraryTable table){
		this.port=port;
		this.table=table;
	}
	public void run(){
		try {
			ServerSocket listener=new ServerSocket(port);
			while(true){
				new LibraryServerThread(listener.accept(),table).start();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
