package co.zhangbiao.zookeeper.curator;

import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.zookeeper.WatchedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Create By ZhangBiao
 * 2020/3/20
 */
public class MyCuratorWatcher implements CuratorWatcher {

    private static final Logger logger = LoggerFactory.getLogger(MyCuratorWatcher.class);

    @Override
    public void process(WatchedEvent watchedEvent) throws Exception {
        logger.info("触发Watcher事件，路径为：{}", watchedEvent.getPath());
    }

}
