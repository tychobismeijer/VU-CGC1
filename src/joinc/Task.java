package joinc;

/**
 * This class contains the information needed to start a new job. 
 */
public class Task {
    
    private static int nextJobNumber = 0;
    
    /**
     * A unique number for the task. 
     */
    public final int taskNumber; 
    
    /**
     * Contains the name of the class that must be started to run the task.
     */
    public final String className;

    /**
     * Contains the name of the file to which the standard output stream should
     * be written.
     */
    public final String stdoutFile;

    /**
     * Contains the name of the file to which the standard error stream should 
     * be written.
     */
    public final String stderrFile;
    
    /**
     * Contains the name of the file from which the standard input stream should
     * be read.
     */
    public final String stdinFile;
    
    /**
     * Contains all command line parameters that need to be passed when the task 
     * is started.
     */         
    public final String [] parameters;

    /**
     * Contains all the names of all jar files that are needed to run the task.
     */
    public final String [] jars;

    /**
     * Contains the names of all input files that the task needs.
     */    
    public final String [] inputFiles;

    /** 
     * Contains the names of all output files that are produced by the task.
     */
    public final String [] outputFiles;
    	   
    /**
     * Construct a new Task
     * 
     * @param className 
     * @param stdoutFile
     * @param stderrFile
     * @param stdinFile
     * @param parameters
     * @param jars
     * @param inputFiles
     * @param outputFiles
     */
    public Task(String className, String stdoutFile, String stderrFile,
            String stdinFile, String[] parameters, String [] jars, 
            String [] inputFiles, String [] outputFiles) {
        
        this.taskNumber = nextJobNumber++;
        this.className = className;
        this.stdoutFile = stdoutFile;
        this.stderrFile = stderrFile;
        this.parameters = parameters;
        this.jars = jars;
        this.inputFiles = inputFiles; 
        this.outputFiles = outputFiles;
        this.stdinFile = stdinFile;
    }
    
}
