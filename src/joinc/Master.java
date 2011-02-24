package joinc;

import java.util.Hashtable;

import org.gridlab.gat.GAT;
import org.gridlab.gat.URI;
import org.gridlab.gat.Preferences;

import org.gridlab.gat.io.File;

import org.gridlab.gat.resources.ResourceBroker;
import org.gridlab.gat.resources.ResourceDescription;
import org.gridlab.gat.resources.HardwareResourceDescription;
import org.gridlab.gat.resources.JavaSoftwareDescription;
import org.gridlab.gat.resources.JobDescription;
import org.gridlab.gat.resources.Job;

/**
 * This is the main class of the JOINC library.
 * 
 * It mainly consists of abstract methods, which will be implemented by 
 * application. Since these methods are the interface to the outside world, 
 * they may NOT be changed! For an example of how they are used, see the 
 * applications.      
 *  
 * @author Jason Maassen
 * @version 2.0 Feb 13, 2006
 * @since 1.0
 * 
 */
public abstract class Master {
        
    /**
     * Returns a task that can be run on the grid.
     * (This method will be implemented by the application).
     * 
     * @return a task to run on the grid.
     */
    public abstract Task getTask(); 
    
    /**
     * This method will be implemented by the application. 
     * It must be called by your code after a task is done 
     * to notify the application of this fact.
     *
     * @param task The task that has finished.
     */
    public abstract void taskDone(Task task); 
        
    /**
     * Returns the total number of tasks that need to be run.
     * (This method will be implemented by the application).
     *  
     * @return number of tasks that will be produced.
     */
    public abstract int totalTasks();
    
    /**
     * Returns the maximum number of workers that may be used.
     * (This method will be implemented by the application). 
     *      
     * @return the maximum number of workers you may use.
     */
    public abstract int maximumWorkers(); 
        
    /**
     * If there is nothing else to do, you may call this method 
     * to allow the application to do some work.
     * (This method will be implemented by the application).
     */
    public abstract void idle();  
    
    /**
     * This is the method you have to implement. By using the
     * abstract methods above you can get information and jobs
     * from the application. Do whatever you need to do here
     * to run these jobs on 'the grid'. Don't forget to notify
     * the application every time a job has finished.
     * 
     * Keep in mind that machines or jobs may fail, so you may
     * need to restart them sometimes. Also, you should not use
     * more workers than what 'maximumWorkers()' returns.
     * 
     * Enjoy! 
     */
    public void start() { 
        try {
            File stdout = GAT.createFile("any:///stdout");
            File stderr = GAT.createFile("any:///stderr");

            Task t = getTask();
            JavaSoftwareDescription sd = new JavaSoftwareDescription();

            sd.setJavaClassPath(t.classPath());
            sd.setJavaMain(t.className);
            sd.setJavaArguments(t.parameters);
            sd.setStdout(stdout);
            sd.setStderr(stderr);

            Preferences prefs = new Preferences();
            //prefs.put("resourcebroker.adaptor.name", "commandlinessh");
            prefs.put("ResourceBroker.adaptor.name", "globus");

            ResourceBroker broker = GAT.createResourceBroker(prefs, new URI("any://fs0.das3.cs.vu.nl/jobmanager-sge"));

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
                    System.out.println(job.toString());                    
            } else { 
                    System.out.println("OK");
                    taskDone(t);
            }

            GAT.end();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }          
}
