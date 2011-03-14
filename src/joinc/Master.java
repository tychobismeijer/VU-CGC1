package joinc;

import java.util.Hashtable;
import java.util.ArrayList;
import java.util.ListIterator;
import java.util.List;
import java.util.Arrays;

import java.net.URISyntaxException;

import org.gridlab.gat.GAT;
import org.gridlab.gat.URI;
import org.gridlab.gat.Preferences;
import org.gridlab.gat.GATObjectCreationException;
import org.gridlab.gat.GATInvocationException;

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
 * 
 */
public abstract class Master
{
    /* Abstract methods *******************************************************
        As given by the example Master of the exercise.
    */
     
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

    
    /* Private Constants and Fields *******************************************
    */

    private List<Task> scheduledTasks;
    private List<Task> currentTasks;
    private List<Job> currentJobs;
    private int finishedTasks;
    private long lastSubmitTime;
    private int currentBroker = 0;
    
    private static final long MAX_SCHEDULE_TIME = 25000000000L;
    private static final int MAX_TASKS_IN_JOB = 2;
    private static final String LOCAL_BROKER = "any://localhost";
    private static final String[] REMOTE_BROKERS = {
        "any://fs0.das3.cs.vu.nl/jobmanager-sge",
        //"any://fs1.das3.liacs.nl/jobmanager-sge",
        //"any://fs2.das3.science.uva.nl/jobmanager-sge",
        //"any://fs3.das3.tudelft.nl/jobmanager-sge",
        //"any://fs4.das3.science.uva.nl/jobmanager-sge"
        };

    /* Public Methods *********************************************************
    */
    
    /** Starts the scheduler.
     */
    public void
    start()
    {
        ResourceBroker[] brokers;
        boolean didSomething;
    
        currentTasks = new ArrayList<Task>();
        scheduledTasks = new ArrayList<Task>();
        currentJobs = new ArrayList<Job>();
        lastSubmitTime = System.nanoTime();
        
        try
        {
            copyGAT();
            brokers = constructBrokers();
            finishedTasks = 0;
            while ((totalTasks() > finishedTasks)
                   || (currentJobs.size() > 0) )
                  // There are Tasks or Jobs left.
            {
                didSomething = false;
                didSomething = (scheduleTasks() || didSomething);
                didSomething = (monitorTasks() || didSomething);
                didSomething = (monitorJobs() || didSomething);
                didSomething = (submitTasks(brokers) || didSomething);

                if (!didSomething)
                {
                    System.err.println("Idle... ["
                        + "scheduled: " + scheduledTasks.size() + ", "
                        + "current: " + currentTasks.size() + ", "
                        + "finished: " + finishedTasks + "]" );
                    idle();
                }
            }
        }
        catch (JOINCException e)
        {
            e.printStackTrace();
        }
        finally
        {
            GAT.end();
        }
    }

    private ResourceBroker[]
    constructBrokers() throws JOINCException
    {
        Preferences prefs;
        ResourceBroker[] brokers = new ResourceBroker[REMOTE_BROKERS.length];
        
        prefs = new Preferences();
        //prefs.put("resourcebroker.adaptor.name", "commandlinessh");
        prefs.put("ResourceBroker.adaptor.name", "globus");
        prefs.put("File.adaptor.name", "local, commandlinessh");
        //prefs.put("globus.sandbox.gram", "true");
        //prefs.put("resourcebroker.adaptor.name", "wsgt4new");
        //prefs.put("wsgt4new.factory.type", "SGE"); 
        for (int i=0; i < REMOTE_BROKERS.length; i++)
        {
            try
            {
                brokers[i] = GAT.createResourceBroker(prefs,
                    new URI(REMOTE_BROKERS[i]) );
            }
            catch (GATObjectCreationException e)
            {
                throw new JOINCException("Could not create resourcebroker",
                    e );
            }
            catch (URISyntaxException e)
            {
                throw new JOINCException(
                    "Could not create resourcebroker, invalid URI: "
                        + REMOTE_BROKERS[i],
                    e );
            }
        }

        return brokers;
    }
    
    private boolean
    scheduleTasks()
    {
        boolean didSchedule = false;
        
        // TODO: only add to queue if the queue is not to full
        while (totalTasks()
               > (finishedTasks
                  + currentTasks.size()
                  + scheduledTasks.size() ) )
        {
            Task newTask = getTask();
            scheduledTasks.add(newTask);
            didSchedule = true;
            System.err.println("Scheduled a Task");
        }

        return didSchedule;
    }
    
    private boolean
    monitorTasks()
    {
        ListIterator<Task> taskiter;

        taskiter = currentTasks.listIterator();
        Task t;
        while (taskiter.hasNext())
        {
            t = taskiter.next();
            Job.JobState state = t.job().getState();
            //System.err.println("a Tasks is in state" + state);
            if (state == Job.JobState.SUBMISSION_ERROR
                || (state == Job.JobState.STOPPED
                    && !t.finishedCorrectly() ) )
            {
                System.err.println("ERROR, Rescheduling a Task");
                scheduledTasks.add(t);
                taskiter.remove();
                return true;
            }
            else if (state == Job.JobState.STOPPED
                     && t.finishedCorrectly())
            {
                System.err.println("Finished task: " + t.taskNumber);
                taskDone(t);
                finishedTasks++;
                taskiter.remove();
                return true;
            }
        }
        return false;
    }
    
    private void
    copyGAT() throws JOINCException
    {
        URI brokerURI;
        URI remoteGAT_URI;
        File remoteGAT;
        File localGAT;
        
        for (String broker : REMOTE_BROKERS)
        {
            try
            {
                brokerURI = new URI(broker);
            }
            catch (URISyntaxException e)
            {
                System.err.println("Could not copy GAT to broker: "
                    + broker
                    + ". Invalid URI");
                continue;
            }
            try
            {
                remoteGAT_URI = new URI("any://"
                    + brokerURI.getHost()
                    + "//var/scratch/tbr440/JavaGAT-2.1.0/" );
            }
            catch (URISyntaxException e)
            {
                System.err.println("Could not copy GAT to broker: "
                    + broker
                    + ". Constructed invalid URI");
                continue;
            }
            try
            {
                remoteGAT = GAT.createFile(remoteGAT_URI);
                if (remoteGAT.exists())
                {
                    System.err.println(
                        "Found JavaGAT at "
                        + remoteGAT_URI.toString() );
                    continue;
                }
                /*else
                {
                    System.err.println(
                        "Remote JavaGAT doesn't exist"
                        + remoteGAT.toString() );
                }*/
            }
            catch (GATObjectCreationException e)
            {
                System.err.println(
                    "failed to create remote GAT file: "
                    + remoteGAT_URI );
            }
            try
            {
                System.err.println("Copying JavaGAT to: "
                    + brokerURI.getHost() );
                localGAT = GAT.createFile("any://localhost//var/scratch/tbr440/JavaGAT-2.1.0/");
                localGAT.copy(remoteGAT_URI);
            }
            catch (GATObjectCreationException e)
            {
                throw new JOINCException(
                    "Could not find local JavaGAT",
                    e );
            }
            catch (GATInvocationException e) 
            {
                throw new JOINCException(
                    "Could not copy JavaGAT",
                    e );
            }
        }
    }
    private boolean
    monitorJobs()
    {
        ListIterator<Job> jobiter;
        Job j;
        boolean didRemoveJob = false;

        jobiter = currentJobs.listIterator();
        while (jobiter.hasNext())
        {
            j = jobiter.next();
            Job.JobState state = j.getState();
            if (state == Job.JobState.SUBMISSION_ERROR)
            {
                System.err.println("ERROR");
                jobiter.remove();
                didRemoveJob = true;
            }
            else if (state == Job.JobState.STOPPED)
            {
                System.err.println("Finished a Job");
                jobiter.remove();
                didRemoveJob = true;
            }
        }

        return didRemoveJob;
    }
    
    /**
     @return boolean wether a task was submitted
     */
    private boolean submitTasks(ResourceBroker[] brokers) throws JOINCException
    {
        List<JobDescription> jobList = new ArrayList<JobDescription>();
        WrapperJob wj;
        WrapperJobDescription wjd;
        int submitSize;
        Task[] t;
        JobDescription[] jobs;

        // Check wether we want and can submit a new job.
        if (scheduledTasks.size() == 0) {
            // There is nothing to schedule
            return false;
        }
        if (currentJobs.size() >= maximumWorkers()) {
            // There is no room to schedule
            return false;
        }
        if (scheduledTasks.size() < MAX_TASKS_IN_JOB &&
                Math.abs(System.nanoTime() - lastSubmitTime) < MAX_SCHEDULE_TIME) {
            // We dont't have enough tasks to fill a job and have time to wait for other tasks.
            System.err.println("Waiting for more tasks");
            return false;
        }
    
        // Calculate the number of tasks in the job we are going to submit.
        submitSize = Math.min(MAX_TASKS_IN_JOB, scheduledTasks.size());
        
        // Setup wrapperjob.
        Preferences prefs = new Preferences();
        prefs.put("ResourceBroker.adaptor.name", "local");
        //prefs.put("File.adaptor.name", "local");
        WrapperSoftwareDescription wsd = new WrapperSoftwareDescription();
        wsd.setExecutable("/usr/local/package/jdk1.6.0-linux-amd64/bin/java");
        wsd.setGATLocation("/var/scratch/tbr440/JavaGAT-2.1.0/");
        
        // Get the first submitSize tasks in the queue, prepare them and put
        // them in t, make jobDescription of them and put these in jobs. Als
        // create a wrapper software description.
        t = new Task[submitSize];
        jobs = new JobDescription[submitSize];
        for (int i=0; i<submitSize; i++)
        {
            t[i] = scheduledTasks.get(i);
            setupInputFiles(t[i].jars, wsd);
            setupInputFiles(t[i].inputFiles, wsd);
            setupOutputFiles(t[i].outputFiles, wsd);
            JobDescription jd = jobDescriptionOf(t[i]);
            t[i].setJobDescription(jd);
            jobs[i] = jd;
        }

        // Add all the jobdescription of the tasks to the wrapper job.
        wjd = new WrapperJobDescription(wsd);
        for (JobDescription jd : jobs)
        {
            try
            {
                wjd.add(jd, new URI(LOCAL_BROKER), prefs);
            }
            catch (URISyntaxException e)
            {
                throw new JOINCException(
                    "Could not add a Task to Job, invalid local broker URI",
                    e );
            }
        }
        
        // Try to submit a job.
        try
        {
            wj = (WrapperJob) brokers[currentBroker].submitJob(wjd);
            
            currentBroker++;
            if (currentBroker >= brokers.length)
            {
                currentBroker = 0;
            }    
        }
        catch (GATInvocationException e)
        {
            System.err.println("Could not submit Job to: " + brokers[currentBroker]);
            e.printStackTrace();
            currentBroker++;
            if (currentBroker >= brokers.length)
            {
                currentBroker = 0;
            }
            return false;
        }
    
        // If we are here there was no error in the submission process.
        
        System.err.println("Submitted a Job to: " + brokers[currentBroker]);
        currentJobs.add(wj);
        
        // Set the wrapper job as the job for the task.
        for (int i=0; i<t.length; i++)
        {
            t[i].setJob(wj);
            scheduledTasks.remove(t[i]);
            currentTasks.add(t[i]);
        }

        // Try to submit more tasks. Only set submittime if a submission was not
        // wanted or went wrong.
        if (!submitTasks(brokers))
        {
            lastSubmitTime = System.nanoTime();
        }
        
        return true;
    }

    private JobDescription jobDescriptionOf(Task t) {
        SoftwareDescription sd = new SoftwareDescription();
        /*
        sd.addAttribute(sd.SANDBOX_ROOT,
            "/local" );
        sd.addAttribute(sd.SANDBOX_USEROOT,
            "false" );
        */
        
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
        try
        {
            File stdout = GAT.createFile("any:///stdout." + t.taskNumber);
            File stderr = GAT.createFile("any:///stderr." + t.taskNumber);
            sd.setStdout(stdout);
            sd.setStderr(stderr);
        }
        catch (GATObjectCreationException e)
        {
            System.err.println("Could not create stdout and stderr");
        }
        
        //Construct JobDescription
        ResourceDescription rd = new HardwareResourceDescription(new Hashtable<String,Object>());
        JobDescription jd = new JobDescription(sd, rd);
        
        return jd;
    }
    
    private void
    setupInputFiles(String[] filenames, SoftwareDescription sd)
    {
        File f;
        
        for (String filename : filenames)
        {
            try
            {
                f = GAT.createFile("any:///"+filename);
                sd.addPreStagedFile(f);
            }
            catch (GATObjectCreationException e)
            {
                System.err.println("Could not create remote file'"
                    + filename +"'" );
                e.printStackTrace();
                /* NOT USED
                throw new JOINCException(
                    "Could not create remote file'" + filename +"'",
                    e);
                */
            }
        }
    }
    
    private void
    setupOutputFiles(String[] filenames, SoftwareDescription sd)
    {
        File f;
        
        for (String filename : filenames)
        {
            try
            {
                f = GAT.createFile("any:///"+filename);
                sd.addPostStagedFile(f);
            }
            catch (GATObjectCreationException e)
            {
                System.err.println("Could not create remote file'"
                    + filename +"'" );
                e.printStackTrace();
                /* NOT USED
                throw new JOINCException(
                    "Could not create remote file'" + filename +"'",
                    e);
                */
            }
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
