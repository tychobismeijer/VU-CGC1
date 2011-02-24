import org.gridlab.gat.GAT;
import org.gridlab.gat.URI;
import org.gridlab.gat.io.File;

class LocalCopy {

	public static void main(String[] args) {

		try { 
			GAT.createFile(new URI("file:///test.in")).copy(new URI("file:///test.copy"));

			System.err.println("OK");

			GAT.end();

		} catch (Exception e) {
			System.err.println("Failed: " + e);
			e.printStackTrace();
		}	
	}
}
