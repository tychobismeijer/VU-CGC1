import java.util.Hashtable;

import org.gridlab.gat.*;
import org.gridlab.gat.io.*;
import org.gridlab.gat.resources.*;

public class RemoteJob { 

	public static void main(String [] args) throws Exception { 
		
		File stdout = GAT.createFile("any:///stdout");
		File stderr = GAT.createFile("any:///stderr");

		SoftwareDescription sd = new SoftwareDescription();

		sd.setExecutable("/bin/hostname");
                sd.setStdout(stdout);
                sd.setStderr(stderr);
		sd.setArguments(new String [] { "-f" });

		Preferences prefs = new Preferences();
		//prefs.put("resourcebroker.adaptor.name", "commandlinessh");
		prefs.put("ResourceBroker.adaptor.name", "globus");

		ResourceBroker broker = GAT.createResourceBroker(prefs, new URI("any://"+args[0]+"/jobmanager-sge"));

		ResourceDescription rd = new HardwareResourceDescription(new Hashtable<String,Object>());
		JobDescription jd = new JobDescription(sd, rd);

		Job job = broker.submitJob(jd);

		Job.JobState state = job.getState();

		while (state != Job.JobState.STOPPED && state != Job.JobState.SUBMISSION_ERROR) {
			try { 
				System.out.println("Sleeping!");
				Thread.sleep(1000);
			} catch (Exception e) { 
				// ignore
			}
			state = job.getState();
		}

		if (state == Job.JobState.SUBMISSION_ERROR) {
			System.out.println("ERROR");			
		} else { 
			System.out.println("OK");
		}

		GAT.end();
	} 
}
