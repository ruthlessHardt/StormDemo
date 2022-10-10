package com.ruth.stormDemo;

import lombok.extern.slf4j.Slf4j;
import org.apache.storm.utils.Utils;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;

/**
 * starting program
 * @author ruthlessHardt
 */
@Slf4j
@SpringBootApplication(exclude = { DataSourceAutoConfiguration.class })
public class StormDemoApplication
{
    private static InetAddress addr;
    public static void main(String[] args) throws Exception {
        SpringApplication.run(StormDemoApplication.class, args);
        Properties pro = System.getProperties();
        Runtime r = Runtime.getRuntime();
        try {
            addr = InetAddress.getLocalHost();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        System.out.println();
        log.info("operating system {}" , pro.getProperty("os.name"));
        log.info("Server CPU {} ",r.availableProcessors());
        log.info("ip {}",addr.getHostAddress());
        log.info("Local host name {}",addr.getHostName());
        log.info("Java version {}",pro.getProperty("java.version"));
        log.info("JVM total memory {} M",r.totalMemory()/1024L/1024L);
        log.info("JVM remaining memory {} M",r.freeMemory()/1024L/1024L);
        log.info("JVM CPU number {} ",r.availableProcessors());
        System.out.println("========================================");
        System.out.println("\nStorm Data analysis project is successfully started\n");
        System.out.println("========================================");

        Utils.sleep(2000);

        Test.main(null);
    }
}
