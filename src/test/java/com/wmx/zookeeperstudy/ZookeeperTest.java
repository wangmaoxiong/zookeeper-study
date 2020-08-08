package com.wmx.zookeeperstudy;

import org.apache.zookeeper.*;
import org.apache.zookeeper.client.ZKClientConfig;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Scanner;

/**
 * 演示对 ZK 基本的增删改查，以及节点监听
 *
 * @author wangMaoXiong
 * @version 1.0
 * @date 2020/8/3 19:28
 */
public class ZookeeperTest {

    private static Logger logger = LoggerFactory.getLogger(ZookeeperTest.class);

    private static ZooKeeper zooKeeper = null;
    /**
     * 服务器连接地址，多个集群时，用逗号隔开，如：192.168.44.41:2181,192.168.44.42:2181,192.168.44.43:2181
     * ZK 服务器默认对客户端监听端口为  2181.
     */
    private static String connectString = "192.168.116.128:2181";

    /**
     * ZooKeeper(String connectString, int sessionTimeout, Watcher watcher)，初始化 ZooKeeper
     * connectString：服务器连接地址，多个集群时，用逗号隔开，如：192.168.44.41:2181,192.168.44.42:2181,192.168.44.43:2181
     * sessionTimeout：会话超时（毫秒）。在 sessionTimeout 内没有心跳检测，会话就标记为失效的
     * watcher：设置观察者对象，接收节点变化通知。不监听时，可以传入 null。
     */
    @Before
    public void init() {
        try {
            System.out.println("开始连接 Zookeeper 服务器【" + connectString + "】");
            zooKeeper = new ZooKeeper("192.168.116.128:2181", 30000, new Watcher() {
                /**
                 * KeeperState getState(): 事件中 ZooKeeper 可能处于的状态的枚举
                 * SyncConnected：客户端处于已连接状态-它连接到集群中的服务器
                 * Closed：客户端已关闭。此状态不由服务器生成，而是在客户端调用{@link ZooKeeper#close()}或{@link ZooKeeper#close(int)}时在本地生成
                 *
                 * @param event
                 */
                @Override
                public void process(WatchedEvent event) {
                    System.out.println("事件类型：" + event.getType());
                    System.out.println("事件发生的路径：" + event.getPath());
                    System.out.println("通知状态：" + event.getState());

                    try {
                        /**默认 watch 监视只会生效一次，如果要想实现永久监听，则需要在上一次监听回调之后再次设置同样的监听
                         * getData 的 watch 为 true 相当于 zk 客户端命令 get -w path，节点值变化时自动触发
                         * getChildren 的 watch 为 true 相当于 zk 客户端命令 ls -w path，节点下的子节点新增获取删除时触发
                         * 只要想重复监视的节点，都可以再次继续监视，比如 getData、getChildren、exists 等等
                         */
                        if (event.getPath() != null) {
                            zooKeeper.getData(event.getPath(), true, null);
                            zooKeeper.getChildren(event.getPath(), true);
                            zooKeeper.exists(event.getPath(), true);
                        }
                    } catch (KeeperException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * synchronized void close()
     * <p>
     * 1、关闭此客户端对象，客户机关闭后，其会话将变为无效。
     * 2、ZooKeeper 服务器中与会话关联的所有临时节点都将被删除，这些节点（以及它们的父节点）上的监视将被触发。
     * </p>
     * boolean close(int waitForShutdownTimeoutMs)
     * <p>
     * 1、此方法将等待内部资源释放。
     * 2、waitForShutdownTimeoutMs：等待资源释放的超时（毫秒），使用零或负值跳过等待
     * 3、如果 waitForShutdownTimeout 大于零并且所有资源都已释放，则返回 true
     * </p>
     */
    @After
    public void destroy() {
        try {
            System.out.println("开始断开与 Zookeeper 的连接【" + connectString + "】");
            boolean close = zooKeeper.close(30 * 1000);
            System.out.printf("连接断开【%s】%n", close);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 测试一：检测 {@link ZooKeeper} 是否连接成功
     * States getState(): 获取客户端连接的状态，States 是一个枚举，有：CONNECTING,ASSOCIATING,CONNECTED,CONNECTEDREADONLY,CLOSED,AUTH_FAILED,NOT_CONNECTED;
     * long getSessionId(): 此 ZooKeeper 客户端实例的会话 id, 返回的值在客户端连接到服务器之前无效，并且在重新连接后可能会更改。
     * List<String> getEphemerals(): 同步获取此会话创建的所有临时节点
     */
    @Test
    public void getState1() {
        try {
            ZKClientConfig clientConfig = zooKeeper.getClientConfig();
            //clientConfig=org.apache.zookeeper.client.ZKClientConfig@1a1d6a08
            logger.info("clientConfig={}", clientConfig);
            //state=CONNECTING
            logger.info("state={}", zooKeeper.getState());
            //sessionId=0
            logger.info("sessionId={}", zooKeeper.getSessionId());
            //getEphemerals=[]
            logger.info("getEphemerals={}", zooKeeper.getEphemerals());
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * String create(final String path,byte[] data,List<ACL> acl,CreateMode createMode)
     * 使用给定路径创建节点，创建成功时，返回创建的节点路径(path)
     * 1、path：节点的路径
     * <p>
     * 1.1、如果路径已经存在，则抛出异常：NodeExistsException
     * 1.2、如果父节点不存在，则抛出异常：NoNodeException
     * </p>
     * 2、data：节点的初始数据。数据的大小不能超出 1 MB（1048576字节），否则抛异常。
     * 3、acl: 创建节点后节点的权限，OPEN_ACL_UNSAFE：表示这是一个完全开放的节点
     * 4、createMode：指定要创建的节点是短暂的还是持久的
     * <span>
     * 4.1、PERSISTENT: 永久节点，不带序列号
     * 4.2、PERSISTENT_SEQUENTIAL：永久节点，带序列号。序列号总是固定长度的 10 位数字，用0填充
     * 4.3、EPHEMERAL：临时节点，不带序列号。临时节点不能有子节点，且在断开连接后，临时节点自动删除。
     * 4.4、EPHEMERAL_SEQUENTIAL: 临时节点，带序列号。
     * ......
     * </span>
     * Stat exists(String path, boolean watch)
     * <p>
     * 1、返回给定路径的节点的状态，如果不存在这样的节点，则返回 null。
     * 2、如果监视（watch）为 true 且调用成功（未引发异常），则将在具有给定路径的节点上保留一个监视，创建/删除节点或设置节点上的数据的成功操作都将触发监视,
     * 会自动触发 new ZooKeeper 时指定的监视器。
     * </p>
     */
    @Test
    public void create1() {
        try {
            String path1 = "/app1";
            String nodeData = "basic-service";
            // 创建一个永久节点
            Stat exists = zooKeeper.exists(path1, false);
            if (exists == null) {
                String result = zooKeeper.create(path1, nodeData.getBytes(Charset.forName("UTF-8")), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                ///app1
                System.out.println("创建永久无序列根节点：" + result);
                return;
            }
            System.out.printf("节点已经存在:【%s】！%n", exists);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建节点演示 2
     */
    @Test
    public void create2() {
        try {
            String path2 = "/app1/ip1";
            String node2Data = "192.168.3.100";
            Stat exists = zooKeeper.exists(path2, false);
            if (exists == null) {
                //创建一个带序列号的永久子节点
                String result = zooKeeper.create(path2, node2Data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
                //输出：/app1/ip10000000001
                System.out.println("创建带序列号的临时子节点：" + result);
                return;
            }
            System.out.printf("节点已经存在:【%s】！%n", exists);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * byte[] getData(String path, boolean watch, Stat stat)
     * 1、返回给定路径的节点的数据和状态。
     * 2、如果监视（watch）为 true 且调用成功（不引发异常），则将在具有给定路径的节点上保留一个监视，
     * 3、监视将由在节点上修改数据操作触发，会自动触发 new ZooKeeper 时指定的监视器
     * 4、不存在指定路径时，抛出异常
     * 5、stat 对象用于接收状态数据,不需要时可以传入 null.
     */
    @Test
    public void getData() {
        try {
            String path = "/app1";
            Stat exists = zooKeeper.exists(path, false);
            if (exists == null) {
                System.out.printf("节点不存在：【%s】%n", path);
                return;
            }
            //这个 stat 对象用于接收状态数据
            Stat stat = new Stat();
            byte[] bytes = zooKeeper.getData(path, true, stat);
            String data = new String(bytes, Charset.forName("UTF-8"));

            //节点【/app1】数据为【basic-web-service】，stat=【138,164,1596628145250,1596631359590,2,4,0,0,17,0,167
            System.out.printf("节点【%s】数据为【%s】，stat=【%s】%n", path, data, stat);

            //下面是用线程阻塞，方便其它 zk 客户端操作节点时，看上面 new ZooKeeper 指定的监听器能否正常触发。
            Scanner scanner = new Scanner(System.in);
            String nextLine = scanner.nextLine();
            System.out.println(nextLine);

        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * List<String> getChildren(String path, boolean watch)
     * 1、返回给定节点路径（path）的子节点列表。如果 path 节点不存在，则抛出异常
     * 2、如果监视（watch）为 true 且调用成功（没引发异常），则将在具有给定路径的节点上保留一个监视。
     * 3、成功删除给定路径的节点或在节点下创建/删除子节点的操作都将触发监视，会自动触发 new ZooKeeper 时指定的监视器
     * 4、返回的子列表没有排序，也不保证其自然或词法顺序
     * 5、还有很多重载的 getChildren 方法
     */
    @Test
    public void getChildren1() {
        try {
            String path = "/app1";
            Stat exists = zooKeeper.exists(path, false);
            if (exists == null) {
                System.out.printf("父节点不存在：【%s】%n，无法获取子节点！", path);
                return;
            }
            // 取出子目录节点列表
            List<String> children = zooKeeper.getChildren(path, false);
            //[ip10000000000]
            System.out.printf("获取【%s】路径下的子节点列表为【%s】%n", path, children);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Stat setData(final String path, byte[] data, int version) 修改节点树
     * 1、如果给定路径（path）的节点存在，且给定版本（version）与节点的版本匹配（如果给定版本为-1，则匹配任何节点的版本），则设置该节点的数据。
     * 2、返回节点的状态。
     * 3、此操作如果成功，将触发 getData 调用留下的给定路径的节点上的所有监视，会自动触发 new ZooKeeper 时指定的监视器
     * 4、如果指定的节点路径不存在，则抛异常；如果给定的版本不匹配，则抛异常。
     * 5、数据（data）的大小不能超出 1 MB（1048576字节），否则抛异常。
     */
    @Test
    public void setData1() {
        try {
            String path1 = "/app1";
            String nodeData = "basic-web-service";
            if (zooKeeper.exists(path1, false) == null) {
                System.out.printf("节点不存在，无法修改：【%s】%n", path1);
                return;
            }
            //修改节点数据
            Stat stat = zooKeeper.setData(path1, nodeData.getBytes(), -1);
            System.out.printf("修改路径【%s】，返回结果【%s】%n", path1, stat);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * void delete(final String path, int version)
     * void delete(final String path, int version, VoidCallback cb, Object ctx) : delete 的异步版本。
     * 1、删除具有给定路径的节点，给定的版本必须与该节点的版本匹配（如果给定的版本是-1，则它匹配任何节点的版本）
     * 2、如果路径不存在，则抛出异常；如果给定的版本号不匹配，则抛出异常；如果节点下还有子子节点，也会抛出异常。
     * 3、此操作如果成功，将触发 exists API 调用在给定路径的节点上的所有监视，以及 getChildren API 调用留下的父节点上的所有监视。
     */
    @Test
    public void delete1() {
        try {
            String path = "/app1/ip10000000001";
            Stat exists = zooKeeper.exists(path, false);
            if (exists == null) {
                System.out.printf("节点不存在，无法删除不存在的节点:【%s】%n", path);
                return;
            }
            List<String> children = zooKeeper.getChildren(path, false);
            if (children != null && children.size() > 0) {
                System.out.printf("节点下存在子节点，无法删除：【%s】%n", children);
                return;
            }
            zooKeeper.delete(path, -1);
            System.out.printf("删除节点【%s】%n", path);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }
}
