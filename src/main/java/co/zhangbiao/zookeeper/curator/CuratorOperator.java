package co.zhangbiao.zookeeper.curator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.retry.*;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Create By ZhangBiao
 * 2020/3/20
 */
public class CuratorOperator {

    private CuratorFramework client;

    private static final String ZK_SERVER_PATH = "10.11.12.106:2181";

    private static final Logger logger = LoggerFactory.getLogger(CuratorOperator.class);

    /**
     * 实例化ZK客户端
     */
    public CuratorOperator() {
        /**
         * 同步创建zk实例，原生api是异步的
         *
         * curator连接zookeeper的策略：ExponentialBackoffRetry
         * 参数：
         * baseSleepTimeMs：初始sleep的时间
         * maxRetries：最大重试次数
         * maxSleepMs：最大重试时间
         *
         */
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 5);

        /**
         * curator连接zookeeper的策略：RetryNTimes
         * n：重试的次数
         * sleepMsBetweenRetries：每次重试间隔的时间
         */
        RetryNTimes retryPlicy2 = new RetryNTimes(3, 5000);

        /**
         * curator连接zookeeper的策略：RetryOneTime
         * sleepMsBetweenRetry：每次重试间隔的时间
         */
        RetryOneTime retryPolicy3 = new RetryOneTime(3000);

        /**
         * 永远重试，不推荐使用
         */
        RetryForever retryForever = new RetryForever(5000);

        /**
         * curator连接zookeeper的策略：RetryUntilElapsed
         * 参数：
         * maxElapsedTimeMs：最大重试时间
         * sleepMsBetweenRetries：每次重试间隔
         * 重试时间超过maxElapsedTimeMs后，就不再重试
         */
        RetryUntilElapsed retryUntilElapsed = new RetryUntilElapsed(2000, 3000);

        this.client = CuratorFrameworkFactory.builder()
                .connectString(ZK_SERVER_PATH)
                .sessionTimeoutMs(10000)
                .retryPolicy(retryPolicy)
                .namespace("workspace")
                .build();
        this.client.start();
    }

    /**
     * 关闭zk客户端连接
     */
    public void closeZKClient() {
        if (this.client != null) {
            this.client.close();
        }
    }

    public CuratorFramework getClient() {
        return client;
    }

    public void setClient(CuratorFramework client) {
        this.client = client;
    }

    public static void main(String[] args) throws Exception {
        CuratorOperator curatorOperator = new CuratorOperator();
        boolean isZKCuratorStarted = curatorOperator.getClient().isStarted();
        System.out.println("当前客户端的状态：" + (isZKCuratorStarted ? "连接中" : "已关闭"));

        //创建节点
        String path = "/super/imooc";
        /*byte[] data = "superme".getBytes();
        curatorOperator.getClient().create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.PERSISTENT)
                .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
                .forPath(path,data);*/
        //修改节点数据
        /*byte[] data = "abc".getBytes();
        curatorOperator.getClient().setData()
                .forPath(path, data);*/

        //删除节点
        /*curatorOperator.getClient().delete()
                .guaranteed()
                .deletingChildrenIfNeeded()
                .forPath(path);*/

        //获取节点
        /*Stat stat = new Stat();
        byte[] data = curatorOperator.getClient().getData()
                .storingStatIn(stat)
                .forPath("/super/imooc2");
        logger.info("节点：{}数据为：{}Version为：{}", "/super/imooc2", new String(data), stat.getVersion());*/

        //获取子节点
        /*List<String> childrens = curatorOperator.getClient().getChildren()
                .forPath("/super");
        childrens.forEach(System.out::println);*/

        //判断节点是否存在
        /*Stat stat = curatorOperator.getClient().checkExists()
                .forPath("/super");
        logger.info("stat：{}", stat);*/

        //Watcher监听事件
        /*curatorOperator.getClient().getData().usingWatcher(new MyCuratorWatcher()).forPath(
                "/super/imooc2");*/

        //使用NodeCache
        /*NodeCache nodeCache = new NodeCache(curatorOperator.getClient(), "/super/imooc");
        nodeCache.start(true);
        if (nodeCache.getCurrentData() != null) {
            byte[] data = nodeCache.getCurrentData().getData();
            logger.info("NodeCache当前数据为：{}", new java.lang.String(data));
        } else {
            logger.info("NodeCache没有数据");
        }
        nodeCache.getListenable().addListener(() -> {
            if(nodeCache.getCurrentData() == null){
                logger.warn("NodeCache数据为空！");
                return;
            }
            byte[] result = nodeCache.getCurrentData().getData();
            logger.info("节点路径：{}，数据：{}", nodeCache.getPath(), new String(result));
        });*/

        // 为子节点添加Watcher事件
        final PathChildrenCache childrenCache = new PathChildrenCache(curatorOperator.getClient(),
                "/super/imooc", true);
        /**
         * StartModel：初始化方式
         * POST_INITIALIZED_EVENT：异步初始化，初始化之后会触发事件
         * NORMAL：异步初始化
         * BUILD_INITIAL_CACHE：同步初始化
         */
        childrenCache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
        List<ChildData> childDataList = childrenCache.getCurrentData();
        System.out.println("当前数据节点的子节点数据列表：");
        childDataList.forEach(childData -> {
            byte[] data = childData.getData();
            System.out.println(new String(data));
        });
        childrenCache.getListenable().addListener((client, event) -> {
            if (event.getType().equals(PathChildrenCacheEvent.Type.INITIALIZED)) {
                logger.info("子节点初始化OK！");
            }
            if (event.getType().equals(PathChildrenCacheEvent.Type.CHILD_ADDED)) {
                logger.info("添加子节点：{}，子节点数据：{}", event.getData().getPath(), new String(event.getData().getData()));
            }
            if (event.getType().equals(PathChildrenCacheEvent.Type.CHILD_REMOVED)) {
                logger.info("删除子节点：{}", event.getData().getPath());
            }
            if (event.getType().equals(PathChildrenCacheEvent.Type.CHILD_UPDATED)) {
                logger.info("修改子节点：{}，修改子节点数据：{}", event.getData().getPath(),
                        new String(event.getData().getData()));
            }
        });
        Thread.sleep(100000);
        curatorOperator.closeZKClient();
        boolean isStarted = curatorOperator.getClient().isStarted();
        System.out.println("当前客户端的状态：" + (isStarted ? "连接中" : "已关闭"));
    }
}
