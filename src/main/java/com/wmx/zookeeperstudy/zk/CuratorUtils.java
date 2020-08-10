package com.wmx.zookeeperstudy.zk;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.retry.RetryForever;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;

import java.nio.charset.Charset;
import java.util.List;

/**
 * Apache curator 客户端操作 Zookeeper 服务器
 *
 * @author wangMaoXiong
 * @version 1.0
 * @date 2020/8/6 20:20
 */
public class CuratorUtils {
    /**
     * 服务器连接地址，多个服务器进行集群时，用逗号隔开，如：192.168.44.41:2181,192.168.44.42:2181,192.168.44.43:2181
     * ZK 服务器默认对客户端监听端口为  2181.
     */
    private static String connectString = "192.168.116.128:2181";
    /**
     * sessionTimeoutMs：会话超时时间，时间单位为毫秒
     * connectionTimeoutMs：连接超时时间，时间单位为毫秒
     */
    private static int sessionTimeoutMs = 10000;
    private static int connectionTimeoutMs = 10000;
    /**
     * 1、由于 ZooKeeper 是一个共享空间，所以给定集群中的用户应该留在预定义的命名空间中
     * 2、如果创建客户端时设置了名称空间，则后期创建的所有路径都将在此命名空间下面
     * 3、比如创建一个 /app 节点，zk 服务器上实际创建的是 /basic-service/app，命名空间节点会自动创建.
     */
    private static String namespace = "basic-service";

    /**
     * == 创建连接  方式 1：
     */
    public static CuratorFramework getConnect1() {
        /**
         * 设置重试策略：在重试间隔增加睡眠时间的情况下重试一定次数
         * ExponentialBackoffRetry(int baseSleepTimeMs, int maxRetries, int maxSleepMs)
         * 1、baseSleepTimeMs：重试之间等待的初始时间量
         * 2、maxRetries：最大重试次数
         * 3、maxSleepMs：每次重试的最大睡眠时间（毫秒），默认为 Integer.MAX_VALUE
         * </p>
         */
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(5 * 1000, 5);
        /**
         * CuratorFramework ：Zookeeper 框架风格客户端，这是接口
         * CuratorFrameworkFactory：用于创建 CuratorFramework 的工厂方法
         * newClient(String connectString, RetryPolicy retryPolicy)
         * newClient(String connectString, int sessionTimeoutMs, int connectionTimeoutMs, RetryPolicy retryPolicy)
         * <p>
         *      connectString：要连接到的服务器列表
         *      sessionTimeoutMs：会话超时时间，时间单位为毫秒，默认为 60 * 1000（1分钟）
         *      connectionTimeoutMs：连接超时时间，时间单位为毫秒，默认为 15 * 1000（15秒）
         *      retryPolicy：使用的重试策略
         * </p>
         * RetryPolicy: 重试连接时要使用的策略，这是个超接口，其常用实现类如下：
         * <p>
         *      ExponentialBackoffRetry：在重试间隔增加睡眠时间的情况下重试一定次数
         *      BoundedExponentialBackoffRetry：重试次数设置为最大重试次数
         *      RetryForever：永远重试，可设置重试时间间隔
         *      RetryNTimes：重试最多重试次数的策略
         *      RetryOneTime：只重试一次的重试策略
         *      RetryUntilElapsed：在给定的时间段内重试
         *      SessionFailedRetryPolicy：Session 过期的重试策略
         * </p>
         */
        CuratorFramework curatorFramework = CuratorFrameworkFactory.newClient(connectString, sessionTimeoutMs, connectionTimeoutMs, retryPolicy);
        /**
         * void start()：启动客户端，这个方法是异步的，主线程会继续往后面走，连接上服务器会需要几十秒不等，
         * 所以大多数方法（比如 create、delete、setData等等方法）在客户端真正连接成功之前都会阻塞
         * void close()：关闭连接
         */
        curatorFramework.start();
        //CuratorFrameworkState getState()：返回此实例的状态
        System.out.println(curatorFramework.getState());
        System.out.printf("开始连接【%s】......%n", connectString);
        return curatorFramework;
    }

    /**
     * == 创建连接  方式 2： CuratorFrameworkFactory..builder() == 推荐方式
     * 1、通过 .builder 方法创建一个构建器，然后进行链式编程，最后 .build() 方法创建实例
     * 2、CuratorFrameworkFactory.newClient 底层也是采用 builder 的方式，所以下面的方法含义和上面方式1是一样的
     * 3、namespace(String namespace) 方法表示命名空间
     * 由于 ZooKeeper 是一个共享空间，所以给定集群中的用户应该留在预定义的命名空间中;
     * 如果在此处设置了名称空间，则后期创建的所有路径都将在此命名空间下
     * 4、defaultData(byte[] defaultData)：为节点设置默认数据，即创建节点时如果没有设置数据，则使用此默认值
     * 传入 null 时，则默认值也为 null，当是当未调用此方法时，则 defaultData 默认值默认为客户端 IP 地址。
     */
    public static CuratorFramework getConnect2() {
        //如果连接失败，则每 60 秒重试一次
        RetryPolicy retryPolicy = new RetryForever(60 * 1000);
        CuratorFramework curatorFramework = CuratorFrameworkFactory
                .builder()
                .defaultData("0".getBytes())
                .connectString(connectString)
                .sessionTimeoutMs(sessionTimeoutMs)
                .connectionTimeoutMs(connectionTimeoutMs)
                .retryPolicy(retryPolicy)
                .namespace(namespace)
                .build();
        /**
         * void start()：启动客户端，这个方法是异步的，主线程会继续往后面走，连接上服务器会需要几十秒不等，
         * 所以大多数方法（比如 create、delete、setData等等方法）在客户端真正连接成功之前都会阻塞
         * void close()：关闭连接
         */
        curatorFramework.start();
        System.out.printf("开始连接【%s】......%n", connectString);
        return curatorFramework;
    }

    /**
     * ExistsBuilder checkExists()：exists生成器，
     * 用于检测某个节点是否存在，forPath 为 null 时表示节点不存在，否则返回节点详细信息
     *
     * @param path
     */
    public static void checkExists1(String path) {
        try {
            CuratorFramework client = getConnect1();
            Stat stat = client.checkExists().forPath(path);
            System.out.printf("节点【%s】是否存在？【%s】", path, stat);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建节点 1 - 基本创建。默认都是同步创建节点
     * CreateBuilder create()：节点创建生成器
     * T forPath(String path)：使用给定的路径和客户端的默认数据提交当前的构建操作
     * 1、可以使用 {@link CuratorFrameworkFactory.Builder#defaultData(byte[])}) 方法设置节点默认数据，未设置时，默认为客户端 IP 地址
     * 2、这里生成的节点默认为不带序列的永久节点，节点权限默认是开放式的，大家都能访问
     *
     * @param path ：节点路径。zk 没有相对路径这一说，必须以 "/" 开头，从根目录开始
     *             * path 只能逐级创建，即父目录必须存在，否则异常
     * @return
     */
    public static String createNode1(String path) {
        String forPath = null;
        try {
            CuratorFramework client = getConnect2();
            //未设置节点数据时，默认为客户端 ip 地址，比如：192.168.116.1
            forPath = client.create().forPath(path);
            System.out.println("节点创建成功：" + namespace + "/" + forPath);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return forPath;
    }

    /**
     * 创建节点 2 - 指定创建模式。默认都是同步创建节点
     *
     * @param path ：节点路径，如 /summary。
     *             * zk 没有相对路径这一说，必须以 "/" 开头，从根目录开始
     *             * path 只能逐级创建，即父目录必须存在，否则异常
     * @param data ：节点数据，如 "天龙八部".getBytes(Charset.forName("UTF-8")
     * @param mode ：节点创建模式 {@link CreateMode}
     *             * PERSISTENT: 永久节点，不带序列号
     *             * PERSISTENT_SEQUENTIAL：永久节点，带序列号。序列号总是固定长度的 10 位数字，用0填充
     *             * EPHEMERAL：临时节点，不带序列号。临时节点不能有子节点，且在断开连接后，临时节点自动删除。
     *             * EPHEMERAL_SEQUENTIAL: 临时节点，带序列号。
     *             * CONTAINER：容器节点，是有用的特殊用途节点，如 leader、lock 等。当容器的最后一个子节点被删除时，容器将成为服务器在将来某个时候删除的候选节点。
     * @return
     */
    public static String createNode2(String path, byte[] data, CreateMode mode) {
        String forPath = null;
        try {
            CuratorFramework client = getConnect2();
            //T withACL(List<ACL> aclList)：设置ACL列表（默认值为OPEN_ACL_UNSAFE)，即默认节点权限是完全开放式的
            forPath = client.create().withMode(mode).withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE).forPath(path, data);
            System.out.printf("创建节点完成=/%s/%s%n", namespace, forPath);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return forPath;
    }

    /**
     * 创建节点 3 -- 级联创建。默认都是同步创建节点
     *
     * @param path ：节点路径，如 /summary/service/port1
     *             * zk 没有相对路径这一说，必须以 "/" 开头，从根目录开始
     *             * 本方法支持级联创建节点，即父节点不存在时也会一并创建
     * @param data ：节点数据，如 "天龙八部".getBytes(Charset.forName("UTF-8")
     * @param mode ：节点创建模式 {@link CreateMode}
     * @return
     */
    public static String createNode3(String path, byte[] data, CreateMode mode) {
        String forPath = null;
        try {
            CuratorFramework client = getConnect2();
            /**
             * ProtectACLCreateModeStatPathAndBytesable<String> creatingParentContainersIfNeeded()
             * 1、如果父节点未创建，则使用{@link CreateMode#CONTAINER} -容器节点模式创建
             * 2、容器创建是 ZooKeeper 最新版本中的一个新功能。
             * 3、当容器中的最后一个子节点被删除时，容器将成为服务器在将来某个时候删除的候选节点。
             * 4、比如级联创建的临时节点，当客户端失去连接后，则级联创建的容器父节点也会一并消失
             * ProtectACLCreateModeStatPathAndBytesable<String> creatingParentsIfNeeded()
             * 1、如果尚未创建任何父节点，则会一并创建这些节点
             * 2、与上面不同，比如级联创建的临时节点，当客户端失去连接后，级联创建的父节点会留下，临时节点被删除
             */
            forPath = client.create().creatingParentContainersIfNeeded()
                    .withMode(mode)
                    .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
                    .forPath(path, data);
            //创建节点完成=/basic-service//summary/service/port10000000000
            System.out.printf("创建节点完成=/%s/%s%n", namespace, forPath);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return forPath;
    }

    /**
     * 创建节点 4 - 异步创建节点
     * T inBackground(BackgroundCallback callback)
     * 1、重载了几个 inBackground 方法，表示在后台执行操作，即异步创建节点
     * 2、callback 是异步创建的回调函数
     *
     * @param path
     * @param data
     * @param mode
     * @return
     */
    public static String createNode4(String path, byte[] data, CreateMode mode) {
        String forPath = null;
        try {
            CuratorFramework client = getConnect2();
            forPath = client.create()
                    .creatingParentsIfNeeded()
                    .withMode(mode)
                    .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
                    .inBackground(new BackgroundCallback() {
                        @Override
                        public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
                            String eventPath = event.getPath();
                            //节点创建完毕：/basic-service//summary/service/port2
                            System.out.printf("节点创建完毕：/%s/%s%n", namespace, eventPath);
                        }
                    }).forPath(path, data);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return forPath;
    }

    /**
     * 删除节点 1。默认为同步操作。
     * DeleteBuilder delete()：创建节点删除生成器
     * forPath(String path): 使用给定路径提交当前生成操作
     *
     * @param path ：被删除的节点路径，如 /summary/service。
     *             * 注意此节点下不能有子节点，否则异常：KeeperException$NotEmptyException
     *             * 被删除节点必须存在，否则异常：KeeperException$NoNodeException
     */
    public static void deleteNode1(String path) {
        try {
            final CuratorFramework client = getConnect2();
            client.delete().forPath(path);
            System.out.printf("删除节点：/%s/%s/%n", namespace, path);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 删除节点2。默认为同步操作。
     *
     * @param path：被删除的节点路径，如 /summary/ip1
     *                        * 注意此节点下不能有子节点，否则异常：KeeperException$NotEmptyException
     *                        * 被删除节点必须存在，否则异常：KeeperException$NoNodeException
     */
    public static void deleteNode2(String path) {
        try {
            CuratorFramework client = getConnect2();
            /**
             * deletingChildrenIfNeeded：如果子节点存在则删除，比如 /summary/port1
             * 1、如果 port1 存在，则会被删除，而 /summary 下即使没有其它节点，自己也不会被删除删除
             * 2、如果 port1 不存在，则直接异常：KeeperException$NoNodeException
             * withVersion(int version)：根据版本号删除，version 默认为  -1，匹配任何版本，如果版本不匹配，则抛出异常：KeeperException$BadVersionException
             */
            client.delete().deletingChildrenIfNeeded()
                    .withVersion(-1).forPath(path);
            System.out.printf("删除节点：/%s%s%n", client.getNamespace(), path);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 删除节点 3 - 强制删除-级联删除。默认为同步操作。
     *
     * @param path：被删除节点路径。无论节点下是否有子节点，都会强制级联删除
     */
    public static void deleteNode3(String path) {
        try {
            final CuratorFramework client = getConnect2();
            //guaranteed()：解决在服务器上操作可能成功，但在成功将响应返回到客户端之前发生连接失败的边缘情况。
            client.delete().guaranteed().deletingChildrenIfNeeded().withVersion(-1).forPath(path);
            System.out.printf("删除节点：/%s%s%n", client.getNamespace(), path);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 删除节点 - 异步删除
     * inBackground(BackgroundCallback callback)：表示后台进行执行操作，即异步执行，callback 是异步执行完毕后的回调函数
     *
     * @param path
     */
    public static void deleteNode4(String path) {
        try {
            CuratorFramework client = getConnect2();
            //异步删除，带有回调函数
            client.delete()
                    .deletingChildrenIfNeeded()
                    .withVersion(-1)
                    .inBackground(new BackgroundCallback() {
                        @Override
                        public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
                            System.out.printf("删除节点：/%s%n", event.getPath());
                        }
                    }).forPath(path);

            System.out.println("异步删除节点开始。。。。。。");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * 获取节点数据 1
     * GetDataBuilder getData()：创建获取数据构建器
     * storingStatIn(Stat stat)：让操作填充提供的 stat 对象，即将节点的详细信息赋值给此 stat 对象
     * 比如数据的版本号，最后修改时间，数据长度等等。
     *
     * @param path：节点路径，如果节点不存在，则抛出异常：KeeperException$NoNodeException
     */
    public static void getNodeData1(String path) {
        try {
            CuratorFramework client = getConnect2();
            Stat stat = new Stat();
            byte[] bytes = client.getData().storingStatIn(stat).forPath(path);
            String data = new String(bytes, Charset.forName("UTF-8"));
            System.out.printf("获取节点【%s%s】数据为【%s】%n", namespace, path, data);
            System.out.printf("节点详细信息【%s】%n", stat);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取节点数据 2 - 异步获取数据
     *
     * @param path：节点路径，如果节点不存在，则抛出异常：KeeperException$NoNodeException
     */
    public static void getNodeData2(String path) {
        try {
            CuratorFramework client = getConnect2();
            client.getData().inBackground(new BackgroundCallback() {
                @Override
                public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
                    byte[] eventData = event.getData();
                    String data = new String(eventData, Charset.forName("UTF-8"));
                    Stat eventStat = event.getStat();
                    //节点【/ip1】数据为【192.168.116.1】
                    System.out.printf("节点【%s】数据为【%s】%n", event.getPath(), data);
                    //节点详细数据为【243,243,1596944343505,1596944343505,0,0,0,0,13,0,243】
                    System.out.printf("节点详细数据为【%s】%n", eventStat);
                }
            }).forPath(path);
            System.out.println("异步获取节点数据开始......");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取节点下的子节点
     * GetChildrenBuilder getChildren()：创建获取子节点构造器
     * T storingStatIn(Stat stat)：将当前节点的详细信息填充到此 stat 对象中，不需要时可以省略
     * T forPath(String path)：使用给定路径提交当前生成操作
     *
     * @param path ：节点必须存在，否则异常
     */
    public static void getChildrenNode(String path) {
        try {
            CuratorFramework client = getConnect2();
            Stat stat = new Stat();
            List<String> forPath = client.getChildren().storingStatIn(stat).forPath(path);
            for (String children : forPath) {
                System.out.printf("子节点：【%s】%n", children);
            }
            System.out.println("详细信息：" + stat);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 更新节点数据 1
     * SetDataBuilder setData()：创建节点修改构建器
     * withVersion(int version)：表示修改指定版本的节点，默认为 -1 表示匹配任意节点。
     * 如果指定的版本与实际版本不匹配，则抛出异常：KeeperException$BadVersionException
     * 节点数据版本（dataVersion）默认从0开始，每修改一次自动 +1
     *
     * @param path：被更新的节点路径，如 /basic-service/info1，如果节点不存在，则异常：KeeperException$NoNodeException
     */
    public static void updateNodeData1(String path, byte[] data) {
        try {
            CuratorFramework client = getConnect2();
            //返回的 stat 是修改后的节点信息，如版本号、数据长度等
            Stat stat = client.setData().withVersion(-1).forPath(path, data);
            System.out.printf("更新节点【%s】%n", path);
            //详细信息【253,339,1597020759961,1597030709189,2,0,0,0,15,0,253
            System.out.printf("详细信息【%s】%n", stat);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 更新节点数据 2 - 异步更新
     *
     * @param path：节点不存在时，不会抛异常。
     */
    public static void updateNodeData2(String path, byte[] data) {
        try {
            CuratorFramework client = getConnect2();
            //修改或者是新增数据
            client.setData().inBackground(new BackgroundCallback() {
                // 回调函数
                @Override
                public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
                    CuratorEventType eventType = event.getType();
                    String eventPath = event.getPath();
                    System.out.printf("事件类型【%s】%n", eventType);
                    System.out.printf("节点路径【%s】%n", eventPath);
                }
            }).forPath(path, data);
            System.out.printf("开始异步更新节点【%s%s】%n", client.getNamespace(), path);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
