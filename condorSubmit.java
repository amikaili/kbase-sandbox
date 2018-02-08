import condor.*;
import java.net.URL;
import java.net.MalformedURLException;
import java.rmi.RemoteException;
import javax.xml.rpc.ServiceException;

public class CondorSubmitJob
{
        private static ClassAdStructAttr[] buildJobAd(String owner,
                                                 String jobFileLocation,
                                                 int clusterId,
                                                 int jobId)
        {
                String jobOutputFileLocation = jobFileLocation + ".job.out";
                String jobLogFileLocation = jobFileLocation + ".job.log";
                String stdOutLocation = jobFileLocation + ".stdout";
                String stdErrLocation = jobFileLocation + ".stderr";
                String dagmanLockFile = jobFileLocation + ".lock";
                String workingDirectory =
                        jobFileLocation.substring(0, jobFileLocation.lastIndexOf("/"));
                
                ClassAdStructAttr[] jobAd =
                {
                 createStringAttribute("Owner", owner), // Need to insert kbase username@realm here
                 createStringAttribute("Iwd", workingDirectory), // Awe creates a working directory per job (uuid) we may need to generate one, or use the job id.  Not sure if condor will create this directory.  If not, we may need to handle working directory creation in the async runner script. 
                 createIntAttribute("JobUniverse", 5), // Vanilla Universe
                 createStringAttribute("Cmd", "run_async_srv_method.sh"),
                 createIntAttribute("JobStatus", 1), // Idle
                 createStringAttribute("Env",
                                       "_CONDOR_MAX_LOG=0;" +
                                       "_CONDOR_LOG=" + jobOutputFileLocation), //leaving in for example setting env var - not needed for kbase
                 createIntAttribute("JobNotification", 0), // Never
                 createStringAttribute("UserLog", jobLogFileLocation),
                 createStringAttribute("RemoveKillSig", "SIGUSR1"),
                 createStringAttribute("Out", stdOutLocation),
                 createStringAttribute("Err", stdErrLocation),
                 createStringAttribute("ShouldTransferFiles", "NO"), // Using shared FS
                 createExpressionAttribute("Requirements", "TRUE"),
                 createExpressionAttribute("OnExitRemove",
                                           "(ExitSignal =?= 11 || " +
                                           " (ExitCode =!= UNDEFINED && " +
                                           "  ExitCode >=0 && ExitCode <= 2))"),
                 createStringAttribute("Arguments",
                                       "-f -l . -Debug 3 "), // also leaving - we can modify for kbase arguments
                 createIntAttribute("ClusterId", clusterId),
                 createIntAttribute("ProcId", jobId)};

                return jobAd;
        }

        private static ClassAdStructAttr createStringAttribute(String name,
                                                          String value)
        {
                return createAttribute(name, value, ClassAdAttrType.value3);
        }

        private static ClassAdStructAttr createIntAttribute(String name,
                                                       int value)
        {
                return createAttribute(name,
                             String.valueOf(value),
                             ClassAdAttrType.value1);
        }

        private static ClassAdStructAttr createExpressionAttribute(String name,
                                                              String value)
        {
                return createAttribute(name, value, ClassAdAttrType.value4);
        }

        private static ClassAdStructAttr createAttribute(String name,
                                                    String value,
                                                    ClassAdAttrType type)
        {
                ClassAdStructAttr attribute = new ClassAdStructAttr();
                attribute.setName(name);
                attribute.setValue(value);
                attribute.setType(type);
                
                return attribute;
        }
        
        public static void main(String[] arguments)
                throws MalformedURLException, RemoteException, ServiceException
        {
               URL scheddLocation = new URL(arguments[0]);
               String owner = arguments[1];
               String jobFileLocation = arguments[2];

                // Get a handle on a schedd we can make SOAP call on.
                CondorScheddLocator scheddLocator = new CondorScheddLocator();
                CondorScheddPortType schedd =
                        scheddLocator.getcondorSchedd(scheddLocation);
                
                // Begin a transaction, allow for 60 seconds between calls
                TransactionAndStatus transactionAndStatus = schedd.beginTransaction(60);
                
                Transaction transaction = transactionAndStatus.getTransaction();
                // Get a new cluster for the job.
                IntAndStatus clusterIdAndStatus = schedd.newCluster(transaction);
                int clusterId = clusterIdAndStatus.getInteger();
                
                // Get a new Job ID (aka a ProcId) for the Job.
                IntAndStatus jobIdAndStatus = schedd.newJob(transaction, clusterId);
                int jobId = jobIdAndStatus.getInteger();
                
                // Build the Job's ClassAd.
                ClassAdStructAttr[] jobAd = buildJobAd(owner,
                                             jobFileLocation,
                                             clusterId,
                                             jobId);
                
                // Submit the Job's ClassAd.
                schedd.submit(transaction, clusterId, jobId, jobAd);
                
                // Commit the transaction.
                schedd.commitTransaction(transaction);

                // Ask the Schedd to kick off the Job immediately.
                schedd.requestReschedule();
        }
}
