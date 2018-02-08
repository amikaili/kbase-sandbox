import condor.*;
import java.net.URL;
import java.net.MalformedURLException;
import java.rmi.RemoteException;
import javax.xml.rpc.ServiceException;

public class CondorSubmitDAG
{
        private static ClassAdStructAttr[] buildJobAd(String owner,
                                                 String dagFileLocation,
                                                 int clusterId,
                                                 int jobId)
        {
                String dagOutputFileLocation = dagFileLocation + ".dagman.out";
                String dagLogFileLocation = dagFileLocation + ".dagman.log";
                String stdOutLocation = dagFileLocation + ".stdout";
                String stdErrLocation = dagFileLocation + ".stderr";
                String dagmanLockFile = dagFileLocation + ".lock";
                String workingDirectory =
                        dagFileLocation.substring(0, dagFileLocation.lastIndexOf("/"));
                
                ClassAdStructAttr[] jobAd =
                {
                 createStringAttribute("Owner", owner),
                 createStringAttribute("Iwd", workingDirectory),
                 createIntAttribute("JobUniverse", 7), // Scheduler Universe
                 createStringAttribute("Cmd", "/usr/bin/condor_dagman"),
                 createIntAttribute("JobStatus", 1), // Idle
                 createStringAttribute("Env",
                                       "_CONDOR_MAX_DAGMAN_LOG=0;" +
                                       "_CONDOR_DAGMAN_LOG=" + dagOutputFileLocation),
                 createIntAttribute("JobNotification", 0), // Never
                 createStringAttribute("UserLog", dagLogFileLocation),
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
                                       "-f -l . -Debug 3 " +
                                       "-AutoRescue 1 -DoRescueFrom 0 " +
                                       "-Allowversionmismatch " + // Often safe
                                       "-Lockfile " + dagmanLockFile + " " +
                                       "-Dag " + dagFileLocation),
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
               String dagFileLocation = arguments[2];

                // Get a handle on a schedd we can make SOAP call on.
                CondorScheddLocator scheddLocator = new CondorScheddLocator();
                CondorScheddPortType schedd =
                        scheddLocator.getcondorSchedd(scheddLocation);
                
                // Begin a transaction, allow for 60 seconds between calls
                TransactionAndStatus transactionAndStatus = schedd.beginTransaction(60);
                
                Transaction transaction = transactionAndStatus.getTransaction();
                // Get a new cluster for the DAG job.
                IntAndStatus clusterIdAndStatus = schedd.newCluster(transaction);
                int clusterId = clusterIdAndStatus.getInteger();
                
                // Get a new Job ID (aka a ProcId) for the DAG Job.
                IntAndStatus jobIdAndStatus = schedd.newJob(transaction, clusterId);
                int jobId = jobIdAndStatus.getInteger();
                
                // Build the DAG's ClassAd.
                ClassAdStructAttr[] jobAd = buildJobAd(owner,
                                             dagFileLocation,
                                             clusterId,
                                             jobId);
                
                // Submit the DAG's ClassAd.
                schedd.submit(transaction, clusterId, jobId, jobAd);
                
                // Commit the transaction.
                schedd.commitTransaction(transaction);

                // Ask the Schedd to kick off the DAG immediately.
                schedd.requestReschedule();
        }
}
