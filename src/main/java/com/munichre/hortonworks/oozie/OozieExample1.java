package com.munichre.hortonworks.oozie;

import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowJob;

import java.util.Properties;

/**
 * Oozie example 1
 * Created by dzinsou on 5/9/17.
 */
public class OozieExample1 {

    private String oozieUrl;
    private String oozieAppPath;
    private String nameNode;
    private String jobTracker;

    public OozieExample1(String oozieUrl, String oozieAppPath, String nameNode, String jobTracker) {
        this.oozieUrl = oozieUrl;
        this.oozieAppPath = oozieAppPath;
        this.nameNode = nameNode;
        this.jobTracker = jobTracker;
    }

    /**
     * Generate your workflow.
     */
    public void generateWorkflow() {
        // TODO Generate your workflow
        // TODO I recommend to use a XML template located in src/main/resourcesP
    }

    /**
     * Copy your generated workflow to HDFS.
     */
    public void copyWorkflowToHDFS() {
        // TODO Instantiate HDFS client
        // TODO Copy from local FS to HFDS
    }

    /**
     * Run your workflow.
     */
    public void runWorkflow() {
        String queueName = "default";
        String oozieLibpath = nameNode + "/user/oozie/share/lib";

        // get a OozieClient for local Oozie
        OozieClient wc = new OozieClient(oozieUrl);
        wc.createConfiguration();

        // create a workflow job configuration and set the workflow application path
        Properties conf = wc.createConfiguration();
        conf.setProperty(OozieClient.APP_PATH, oozieAppPath);

        // setting workflow parameters
        conf.setProperty("jobTracker", jobTracker);
        conf.setProperty("nameNode", nameNode);
        conf.setProperty("queueName", queueName);
        conf.setProperty("oozie.libpath", oozieLibpath);
        conf.setProperty("oozie.use.system.libpath", "true");
        conf.setProperty("oozie.wf.rerun.failnodes", "true");

        try {
            String jobId = wc.run(conf);
            System.out.println("Workflow job, " + jobId + " submitted");

            while (wc.getJobInfo(jobId).getStatus() == WorkflowJob.Status.RUNNING) {
                System.out.println("Workflow job running ...");
                Thread.sleep(10 * 1000);
            }
            System.out.println("Workflow job completed ...");
            System.out.println(wc.getJobInfo(jobId));
        } catch (Exception r) {
            System.out.println("Errors " + r.getLocalizedMessage());
        }
    }

    public static void main(String[] args) {
        String oozieUrl = args[0];
        String oozieAppPath = args[1];
        String nameNode = args[2];
        String jobTracker = args[3];

        OozieExample1 example1 = new OozieExample1(oozieUrl, oozieAppPath, nameNode, jobTracker);
        example1.generateWorkflow();
        example1.copyWorkflowToHDFS();
        example1.runWorkflow();
    }

}
