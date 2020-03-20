package co.zhangbiao.zookeeper.utils;

import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;

/**
 * Create By ZhangBiao
 * 2020/3/20
 */
public class ACLUtils {

    public static String getDigestUserPwd(String id) throws Exception {
        return DigestAuthenticationProvider.generateDigest(id);
    }

}
