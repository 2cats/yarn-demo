package com.tcats.yarndemo;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;


public class Client {
    private static final String JAR_FILENAME = "SimpleApp.jar";
    private static final String APP_NAME = "yarn-demo";
    private static final int AM_MEMORY = 256;
    private static final int AM_CORES = 1;
    private static final String QUEUE_NAME = "default";

    public static void main(String[] args) throws Exception {
        final String command = args[0];
        final int n = Integer.valueOf(args[1]);
        final String jarFile = args[2];
        System.out.println("[ CMD ] " + command);

        // Create yarnClient
        YarnConfiguration conf = new YarnConfiguration();
        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);
        yarnClient.start();
        YarnClientApplication app = yarnClient.createApplication();

        // Prepare LocalResources , which will be copied to AM's working-dir automatically
        Map<String, LocalResource> localResources = new HashMap<>();
        FileSystem fs = FileSystem.get(conf);
        Path dst = new Path(fs.getHomeDirectory(), "/" + JAR_FILENAME);
        fs.copyFromLocalFile(new Path(jarFile), dst);
        FileStatus jarStat = fs.getFileStatus(dst);
        localResources.put(JAR_FILENAME, LocalResource.newInstance(
                ConverterUtils.getYarnUrlFromPath(dst),
                LocalResourceType.FILE, LocalResourceVisibility.APPLICATION,
                jarStat.getLen(), jarStat.getModificationTime()));
        localResources.forEach((String key, LocalResource val) -> {
            System.out.printf("[ RES ] %s -> %s\n", key, val.getResource().getFile());
        });

        // Setup ENV
        // ENV-CLASSPATH-YARN
        Map<String, String> appMasterEnv = new HashMap<>();
        for (String c : conf.getStrings(
                YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
            Apps.addToEnvironment(appMasterEnv,
                    Environment.CLASSPATH.name(),
                    c.trim(),
                    ApplicationConstants.CLASS_PATH_SEPARATOR);
        }
        // ENV-CLASSPATH-USER-JAR
        Apps.addToEnvironment(appMasterEnv,
                Environment.CLASSPATH.name(),
                Environment.PWD.$() + File.separator + "*",
                ApplicationConstants.CLASS_PATH_SEPARATOR);
        appMasterEnv.forEach((String key, String val) -> {
            System.out.printf("[ ENV ] %s -> %s\n", key, val);
        });

        //Setup Command
        List<String> commands = Collections.singletonList(
                Environment.JAVA_HOME.$$() + "/bin/java" +
                        " -Xmx" + AM_MEMORY + "M" +
                        " " + ApplicationMasterAsync.class.getName() +
                        " '" + command + "'" +
                        " " + n +
                        " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
                        " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr" // web ui log
        );
        // Create AMContainer
        ContainerLaunchContext amContainer = ContainerLaunchContext.newInstance(
                localResources,
                appMasterEnv,
                commands, null, null, null
        );
        // Create SubmissionContext
        ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
        appContext.setApplicationName(APP_NAME);
        appContext.setAMContainerSpec(amContainer);
        appContext.setResource(Resource.newInstance(AM_MEMORY, AM_CORES));
        appContext.setQueue(QUEUE_NAME);

        // Submit application
        ApplicationId appId = appContext.getApplicationId();
        System.out.println("Submitting application " + appId);
        yarnClient.submitApplication(appContext);

        // Monitor application
        ApplicationReport appReport;
        YarnApplicationState appState;
        do {
            Thread.sleep(1000);
            appReport = yarnClient.getApplicationReport(appId);
            appState = appReport.getYarnApplicationState();
        } while (appState != YarnApplicationState.FINISHED &&
                appState != YarnApplicationState.KILLED &&
                appState != YarnApplicationState.FAILED);
        System.out.println("[ END ] " + appId + " " + appState);
    }
}
