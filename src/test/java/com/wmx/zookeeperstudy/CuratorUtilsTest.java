package com.wmx.zookeeperstudy;

import com.wmx.zookeeperstudy.zk.CuratorUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.junit.Test;

import java.nio.charset.Charset;

/**
 * @author wangMaoXiong
 * @version 1.0
 * @date 2020/8/9 9:04
 */
public class CuratorUtilsTest {

    @Test
    public void getConnect1Test1() {
        try {
            CuratorFramework client = CuratorUtils.getConnect1();
            String namespace = client.getNamespace();
            System.out.println("namespace=" + namespace);

            Thread.sleep(Integer.MAX_VALUE);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void getConnect2Test1() {
        try {
            CuratorFramework client = CuratorUtils.getConnect2();
            String namespace = client.getNamespace();
            System.out.println(namespace);

            Thread.sleep(Integer.MAX_VALUE);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void checkExists1Test() {
        CuratorUtils.checkExists1("/basic-service/ip1");
    }

    @Test
    public void createNode1Test() {
        String node1 = CuratorUtils.createNode1("/info1");
        System.out.println(node1);

        try {
            Thread.sleep(Integer.MAX_VALUE);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    @Test
    public void createNode2Test() {
        try {
            String node1 = CuratorUtils.createNode2("/summary", "天龙八部".getBytes(Charset.forName("UTF-8")), CreateMode.PERSISTENT_SEQUENTIAL);
            System.out.println(node1);

            Thread.sleep(Integer.MAX_VALUE);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void createNode3Test() {
        try {
            String node1 = CuratorUtils.createNode3("/summary/service/port1", "8956".getBytes(Charset.forName("UTF-8")), CreateMode.EPHEMERAL_SEQUENTIAL);
            System.out.println(node1);

            Thread.sleep(Integer.MAX_VALUE);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void createNode4Test() {
        try {
            String node1 = CuratorUtils.createNode4("/summary/service/port2", "5675".getBytes(Charset.forName("UTF-8")), CreateMode.EPHEMERAL_SEQUENTIAL);
            System.out.println(node1);

            Thread.sleep(Integer.MAX_VALUE);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void deleteNode1Test() {
        CuratorUtils.deleteNode1("/summary/service");
    }

    @Test
    public void deleteNode2Test() {
        CuratorUtils.deleteNode2("/summary/port1");
    }

    @Test
    public void deleteNode3Test() {
        CuratorUtils.deleteNode3("/summary");
    }

    @Test
    public void deleteNode4Test() {
        try {
            CuratorUtils.deleteNode4("/summary0000000007");
            Thread.sleep(Integer.MAX_VALUE);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void getNodeData1Test1() {
        CuratorUtils.getNodeData1("/ip1");
    }

    @Test
    public void getNodeData1Test2() {
        try {
            CuratorUtils.getNodeData2("/ip1");
            Thread.sleep(Integer.MAX_VALUE);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void getChildrenNode1Test() {
        CuratorUtils.getChildrenNode("/");
    }

    @Test
    public void updateNodeData1Test() {
        CuratorUtils.updateNodeData1("/info1", "145.145.145.144".getBytes(Charset.forName("UTF-8")));
    }

    @Test
    public void updateNodeData2Test() {
        try {
            CuratorUtils.updateNodeData2("/info2", "145.145.145.142".getBytes(Charset.forName("UTF-8")));
            Thread.sleep(Integer.MAX_VALUE);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
