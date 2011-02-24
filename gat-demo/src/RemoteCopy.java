import org.gridlab.gat.GAT;
import org.gridlab.gat.URI;
import org.gridlab.gat.io.File;

class RemoteCopy {

	public static void main(String[] args) {

		try { 
			GAT.createFile(new URI("ssh:///test.in")).copy(new URI("ssh://fs4.das3.science.uva.nl/test.copy"));

			System.err.println("OK");

			GAT.end();

		} catch (Exception e) {
			System.err.println("Failed: " + e);
			e.printStackTrace();
		}	
	}
}
