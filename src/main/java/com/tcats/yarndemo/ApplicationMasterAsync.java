package com.tcats.yarndemo;

import java.io.File;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Records;

// 1. AM Runs On container 1
// 2. AM request RM,NM for containers to run commands
// 3. localResources specified by client will be auto copied
public class ApplicationMasterAsync implements AMRMClientAsync.CallbackHandler {
    private static final int TASK_MEMORY = 256;
    private static final int TASK_CORES = 1;

    private String command;
    private int numContainers;
    private NMClient nmClient;
    private Configuration configuration;

    private ApplicationMasterAsync(String command, int numContainers) {
        this.command = command;
        this.numContainers = numContainers;
        this.configuration = new YarnConfiguration();
        this.nmClient = NMClient.createNMClient();
        this.nmClient.init(this.configuration);
        this.nmClient.start();
    }

    public void onContainersAllocated(List<Container> containers) {
        for (Container container : containers) {
            try {
                ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
                ctx.setCommands(Collections.singletonList(command));
                System.out.println("Launch container " + container.getId() + " -> NM");
                nmClient.startContainer(container, ctx);
            } catch (Exception ex) {
                System.err.println("Error launching container " + container.getId() + " " + ex);
            }
        }
    }

    public void onContainersCompleted(List<ContainerStatus> statuses) {
        for (ContainerStatus status : statuses) {
            System.out.println("Container " + status.getContainerId() + " Completed");
            synchronized (this) {
                numContainers--;
            }
        }
    }

    public void onNodesUpdated(List<NodeReport> updated) {
    }

    public void onShutdownRequest() {
    }

    public void onError(Throwable t) {
    }

    public float getProgress() {
        return 0;
    }

    private void runDebug() {
        File curDir = new File(".");
        System.out.println("CWD : " + curDir.getAbsolutePath());
        File[] filesList = curDir.listFiles();
        if (filesList != null) {
            System.out.println("=== list dir ===");
            for (File f : filesList) {
                if (f.isDirectory() || f.isFile())
                    System.out.println(f.getName());
            }
            System.out.println("================");
        }

        System.out.println("CLASSPATH : " + System.getProperty("java.class.path"));
    }

    public static void main(String[] args) throws Exception {
        final String command = args[0];
        final int n = Integer.valueOf(args[1]);

        ApplicationMasterAsync master = new ApplicationMasterAsync(command, n);
        master.runDebug();
        master.runMainLoop();

    }

    private void runMainLoop() throws Exception {
        AMRMClientAsync<ContainerRequest> rmClient = AMRMClientAsync.createAMRMClientAsync(100, this);
        rmClient.init(this.configuration);
        rmClient.start();

        System.out.println("Register AM -> RM");
        rmClient.registerApplicationMaster("", 0, "");

        System.out.println("Request " + numContainers + " containers -> RM");
        Priority priority = Priority.newInstance(0);
        Resource capability = Resource.newInstance(TASK_MEMORY, TASK_CORES);
        for (int i = 0; i < numContainers; ++i) {
            ContainerRequest containerAsk = new ContainerRequest(capability, null, null, priority);
            rmClient.addContainerRequest(containerAsk);
        }

        System.out.println("Wait for containers to finish");
        while (numContainers > 0) {
            Thread.sleep(1000);
        }

        System.out.println("Unregister ApplicationMaster");
        rmClient.unregisterApplicationMaster(
                FinalApplicationStatus.SUCCEEDED, "", "");
    }
}
