import org.gridlab.gat.GAT;
import org.gridlab.gat.URI;
import org.gridlab.gat.io.File;

class RemoteRemoteCopy {

	public static void main(String[] args) {

		try { 
			if (args.length == 2) { 

				GAT.createFile(new URI(args[0])).copy(new URI(args[1]));

				System.err.println("OK");
	
				GAT.end();
			} else { 
				System.err.println("Please provide source and destination URI's");
			} 
		} catch (Exception e) {
			System.err.println("Failed: " + e);
			e.printStackTrace();
		}	
	}
}
