package joinc;

import java.util.Hashtable;
import java.util.ArrayList;
import java.util.ListIterator;
import java.util.List;
import java.util.Arrays;

import org.gridlab.gat.GAT;
import org.gridlab.gat.URI;
import org.gridlab.gat.Preferences;

import org.gridlab.gat.io.File;

import org.gridlab.gat.resources.ResourceBroker;
import org.gridlab.gat.resources.ResourceDescription;
import org.gridlab.gat.resources.HardwareResourceDescription;
import org.gridlab.gat.resources.WrapperSoftwareDescription;
import org.gridlab.gat.resources.SoftwareDescription;
import org.gridlab.gat.resources.JobDescription;
import org.gridlab.gat.resources.WrapperJobDescription;
import org.gridlab.gat.resources.Job;
import org.gridlab.gat.resources.WrapperJob;

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

    private List<Task> scheduledTasks;
    private List<Task> currentTasks;
    private List<Job> currentJobs;
    private long lastSubmitTime;
    private static final long MAX_SCHEDULE_TIME = 5000000000L;
    private static final int MAX_TASKS_IN_JOB = 1;
    private static final String LOCAL_BROKER = "any://localhost";
    private static final String REMOTE_BROKER = "any://fs0.das3.cs.vu.nl/jobmanager-sge";
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
        currentTasks = new ArrayList<Task>();
        scheduledTasks = new ArrayList<Task>();
        currentJobs = new ArrayList<Job>();
        lastSubmitTime = System.nanoTime();
        
        try {
            Preferences prefs = new Preferences();
            prefs.put("resourcebroker.adaptor.name", "commandlinessh");
            //prefs.put("ResourceBroker.adaptor.name", "globus");
            //prefs.put("globus.sandbox.gram", "true");
            
            ResourceBroker broker = GAT.createResourceBroker(prefs, new URI(REMOTE_BROKER));

            int finishedTasks = 0;
            boolean didSomething;
            ListIterator<Task> taskiter;
            ListIterator<Job> jobiter;
            while(totalTasks() > finishedTasks) {
                didSomething = false;
                // TODO: only add to queue if the queue is not to full
                if ( totalTasks() > (finishedTasks + currentTasks.size() + scheduledTasks.size()) ) {
                    Task newTask = getTask();
                    scheduledTasks.add(newTask);
                    didSomething = true;
                    System.err.println("Scheduled a Task");
                }
                taskiter = currentTasks.listIterator();
                Task t;
                while (taskiter.hasNext()) {
                    t = taskiter.next();
                    Job.JobState state = t.job().getState();
                    if (state == Job.JobState.SUBMISSION_ERROR) {
                        System.err.println("ERROR");
                        didSomething = true;
                        scheduledTasks.add(t);
                        taskiter.remove();
                    } else
                    if (state == Job.JobState.STOPPED) {
                        System.err.println("Finished a Task");
                        taskDone(t);
                        finishedTasks++;
                        didSomething = true;
                        taskiter.remove();
                    }
                }
                jobiter = currentJobs.listIterator();
                Job j;
                while (jobiter.hasNext()) {
                    j = jobiter.next();
                    Job.JobState state = j.getState();
                    if (state == Job.JobState.SUBMISSION_ERROR) {
                        System.err.println("ERROR");
                        didSomething = true;
                        jobiter.remove();
                    } else
                    if (state == Job.JobState.STOPPED) {
                        System.err.println("Finished a Task");
                        didSomething = true;
                        jobiter.remove();
                    }
                }
                didSomething = (submitTasks(broker) || didSomething);
                if (!didSomething) {
                    try { 
                        System.err.println("Sleeping! [" +
                            scheduledTasks.size() + ", " +
                            currentTasks.size() + ", " +
                            finishedTasks + "]"
                        );
                        //Thread.sleep(10000);
                        idle();
                    } catch (Exception e) { 
                            // ignore
                    }
                }
            }
            GAT.end();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private boolean submitTasks(ResourceBroker broker) throws Exception {
        if (scheduledTasks.size() == 0) {
            return false;
        }
        if (currentJobs.size() >= maximumWorkers()) {
            return false;
        }
        if (scheduledTasks.size() < MAX_TASKS_IN_JOB &&
                Math.abs(System.nanoTime() - lastSubmitTime) < MAX_SCHEDULE_TIME) {
            return false;
        }
        Preferences prefs = new Preferences();
        prefs.put("ResourceBroker.adaptor.name", "local");
        WrapperSoftwareDescription wsd = new WrapperSoftwareDescription();
        wsd.setExecutable("/usr/local/package/jdk1.6.0-linux-amd64/bin/java");
        WrapperJobDescription wjd = new WrapperJobDescription(wsd);
        Task[] t = new Task[MAX_TASKS_IN_JOB];
        int i = 0;
        while ((i < MAX_TASKS_IN_JOB) && (scheduledTasks.size() > 0)) {
            t[i] = scheduledTasks.remove(0);
            JobDescription jd = jobDescriptionOf(t[i]);
            t[i].setJobDescription(jd);
            wjd.add(jd, new URI(REMOTE_BROKER), prefs);
            i++;
        }   
        WrapperJob wj = (WrapperJob) broker.submitJob(wjd);
        System.err.println("Submitted a Job");
        currentJobs.add(wj);
        for (i=0; i<t.length; i++) {
            if (t[i] == null) { break; }
            t[i].setJob(wj.getJob(t[i].jobDescription()));
            currentTasks.add(t[i]);
        }
        if(!submitTasks(broker)) {
            lastSubmitTime = System.nanoTime();
        }
        return true;
    }

    private JobDescription jobDescriptionOf(Task t) throws Exception {
        SoftwareDescription sd = new SoftwareDescription();
        
        //Setup commondline
        sd.setExecutable("/usr/bin/java");
        sd.setArguments(cat(
            new String[] {"-classpath"},
            t.jars,
        new String[] {t.className},
            t.parameters));
        
        //Setup Sandbox
        setupInputFiles(t.jars, sd);
        setupInputFiles(t.inputFiles, sd);
        setupOutputFiles(t.outputFiles, sd);
        
        //Setup other task environment
        File stdout = GAT.createFile("any:///stdout");
        File stderr = GAT.createFile("any:///stderr");
        sd.setStdout(stdout);
        sd.setStderr(stderr);
        
        //Construct JobDescription
        ResourceDescription rd = new HardwareResourceDescription(new Hashtable<String,Object>());
        JobDescription jd = new JobDescription(sd, rd);
        
        return jd;
    }
    
    private void setupInputFiles(String[] filenames, SoftwareDescription sd) throws Exception {
        for (String filename : filenames) {
            File f = GAT.createFile("any:///"+filename);
            sd.addPreStagedFile(f); 
        }
    }
    
    private void setupOutputFiles(String[] filenames, SoftwareDescription sd) throws Exception {
        for (String filename : filenames) {
            File f = GAT.createFile("any:///"+filename);
            sd.addPostStagedFile(f);
        }
    }
    
    /** Concatenate four String arrays.
     */
    private static String[] cat(String[] a1, String[] a2, String[] a3, String[] a4) {
        String[] result = new String[a1.length + a2.length + a3.length + a4.length];
        
        System.arraycopy(a1, 0, result, 0, a1.length);
        System.arraycopy(a2, 0, result, a1.length, a2.length);
        System.arraycopy(a3, 0, result, a1.length+a2.length, a3.length);
        System.arraycopy(a4, 0, result, a1.length+a2.length+a3.length, a4.length);
        
        return result;
    } 
}
