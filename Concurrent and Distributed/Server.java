import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.Scanner;
import java.util.StringTokenizer;


public class Server {
	int numbooks;
	int udpport;
	int tcpport;
	LibraryTable table;
	public Server(int numbooks, int udpport, int tcpport){
		this.numbooks=numbooks;
		this.udpport=udpport;
		this.tcpport=tcpport;
		table=new LibraryTable(numbooks);
		}
	public static void main(String[] args){
		Server ls=new Server(Integer.parseInt(args[0]),Integer.parseInt(args[1]),Integer.parseInt(args[2]));
		Thread tcpthread=new Thread(new TCPThread(ls.tcpport,ls.table));
		tcpthread.start();
		
		try {
			DatagramSocket datasocket=new DatagramSocket(ls.udpport);
			DatagramPacket datagram, returnpacket = null;
			int len=1024;
			byte[] buf=new byte[len];
			byte[] rbuf;
			Socket s;
				while(true){
					datagram=new DatagramPacket(buf,len);
					datasocket.receive(datagram);
					String clientmsg=new String(datagram.getData(),0,datagram.getLength());
					//System.out.println("From Server,UDPpart: "+clientmsg);
					Scanner st=new Scanner(clientmsg);
					String clientid = st.next();
					String booknum=st.next();
					String reqret=st.next();
					if(reqret.equals("reserve")){
						if(ls.table.request(booknum, clientid)){
							rbuf=new String(clientid +" "+ booknum).getBytes();
							returnpacket=new DatagramPacket(rbuf,rbuf.length,datagram.getAddress(),datagram.getPort());
							}
						else {
							rbuf=new String("fail "+ clientid +" "+ booknum).getBytes();
							returnpacket=new DatagramPacket(rbuf,rbuf.length,datagram.getAddress(),datagram.getPort());
							}
					}
					else if(reqret.equals("return")){
						if(ls.table.returnBook(booknum, clientid)){
							rbuf=new String("free "+clientid +" "+ booknum).getBytes();
							returnpacket=new DatagramPacket(rbuf,rbuf.length,datagram.getAddress(),datagram.getPort());
							}
						else {
							rbuf=new String("fail "+ clientid +" "+ booknum).getBytes();
							returnpacket=new DatagramPacket(rbuf,rbuf.length,datagram.getAddress(),datagram.getPort());
							}
					}
					datasocket.send(returnpacket);
					
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		/*DatagramSocket datasocket = null;
		DatagramPacket datagram, returnpacket;
		int len=1024;
		
		byte[] buf=new byte[len];
		try {
			datasocket=new DatagramSocket(server.udpport);	
			while(true){
			datagram=new DatagramPacket(buf,len);
			datasocket.receive(datagram);
			String clientmsg=new String(datagram.getData(),0,datagram.getLength());
			System.out.println("From Server: "+clientmsg);
			StringTokenizer tokens=new StringTokenizer(clientmsg);
			if(tokens.countTokens()<3){
				continue;
			}
			int clientid=Integer.parseInt(tokens.nextToken());
			String bookid=tokens.nextToken();
			String reqret=tokens.nextToken();
			
			
			}
		} catch (SocketException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
		
		
	}
}
