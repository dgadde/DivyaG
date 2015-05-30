import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Scanner;


public class Client {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		int len=1024;
		Scanner in=new Scanner(System.in);
		String firstline=in.nextLine();
		Scanner one=new Scanner(firstline);
		String clientid="c"+one.next();
		String ip=one.next();
		one.close();
		Scanner command = null;
		DatagramSocket datasocket = null;
		Socket cSocket = null;
		try {
			datasocket=new DatagramSocket();
			String host=InetAddress.getByName(ip).getHostName();
			while(in.hasNextLine()){
				command=new Scanner(in.nextLine());
				String first=command.next();
				if(first.equals("sleep")){
					Thread.sleep(Integer.parseInt(command.next()));
				}
				else{
					String reqret=command.next();
					int port=Integer.parseInt(command.next());
					String tcporudp=command.next();
					if(tcporudp.equals("T")){
						cSocket = new Socket(host, port);
					    PrintWriter out = new PrintWriter(cSocket.getOutputStream(), true);
					    out.println(clientid+" "+first+" "+reqret);
					    BufferedReader ins = new BufferedReader(new InputStreamReader(cSocket.getInputStream()));
					    System.out.println(ins.readLine());
					    ins.close();
					    out.close();
					}
					else{
						DatagramPacket datapacket,rpacket;
						byte[] rbuf=new byte[len];
						
							byte[] buf=new String(clientid+" "+first+" "+reqret).getBytes();
							datapacket=new DatagramPacket(buf, buf.length,InetAddress.getByName(ip),port);
							String val=new String(datapacket.getData(),0,datapacket.getLength());
							datasocket.send(datapacket);
							rpacket=new DatagramPacket(rbuf,len);
							datasocket.receive(rpacket);
							String retval=new String(rpacket.getData(),0,rpacket.getLength());
							System.out.println(retval);
					
				}
			}
			}
			
			cSocket.close();
			in.close();
			command.close();
			
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			datasocket.close();
			
		}
	

}}
