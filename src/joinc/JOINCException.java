package joinc;

class JOINCException extends Exception
{
    public JOINCException()
    {
        super();
    }

    public JOINCException(String msg)
    {
        super(msg);
    }
    
    public JOINCException(String msg, Throwable cause)
    {
        super(msg, cause);
    }
    
    public JOINCException(Throwable cause)
    {
        super(cause);
    }
}

