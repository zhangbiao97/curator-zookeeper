package co.zhangbiao.zookeeper.curator;

import co.zhangbiao.zookeeper.utils.ACLUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

/**
 * Create By ZhangBiao
 * 2020/3/20
 */
public class CuratorAcl {

    private static final Logger logger = LoggerFactory.getLogger(CuratorAcl.class);

    private CuratorFramework client;

    private static final String ZK_SERVER_PATH = "10.11.12.106:2181";

    public CuratorAcl() {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(5000, 5);
        this.client = CuratorFrameworkFactory.builder()
                .authorization("digest", "zhangbiao1:zhangbiao".getBytes())
                .retryPolicy(retryPolicy)
                .sessionTimeoutMs(10000)
                .connectString(ZK_SERVER_PATH)
                .namespace("workspace")
                .build();
        this.client.start();
    }

    public void close() {
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
        CuratorAcl curatorAcl = new CuratorAcl();
        boolean isStarted = curatorAcl.getClient().isStarted();
        logger.info("当前客户端状态：{}", (isStarted ? "已连接" : "已断开"));
        ArrayList<ACL> acls = new ArrayList<>();
        Id zhangbiao1 = new Id("digest", ACLUtils.getDigestUserPwd("zhangbiao1:zhangbiao"));
        Id zhangbiao2 = new Id("digest", ACLUtils.getDigestUserPwd("zhangbiao2:zhangbiao"));
        acls.add(new ACL(ZooDefs.Perms.ALL, zhangbiao1));
        acls.add(new ACL(ZooDefs.Perms.CREATE, zhangbiao2));
        acls.add(new ACL(ZooDefs.Perms.DELETE | ZooDefs.Perms.READ, zhangbiao2));
        // 设置ACL权限
        curatorAcl.getClient().create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.PERSISTENT)
                .withACL(acls, true)
                .forPath("/acl/imooc");
        Thread.sleep(5000);
        curatorAcl.close();
        boolean isStarted2 = curatorAcl.getClient().isStarted();
        logger.info("当前客户端状态：{}", (isStarted2 ? "已连接" : "已断开"));
    }
}
