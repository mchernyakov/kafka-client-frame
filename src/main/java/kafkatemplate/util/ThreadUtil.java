package kafkatemplate.util;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.ThreadFactory;

public final class ThreadUtil {
    private ThreadUtil() {
    }

    public static ThreadFactory getThreadFactory(String name) {
        return getThreadFactory(name, true);
    }

    public static ThreadFactory getThreadFactoryCollection(String name, boolean daemonFlag) {
        return new ThreadFactoryBuilder()
                .setNameFormat(name + "-%d")
                .setDaemon(daemonFlag)
                .build();
    }

    public static ThreadFactory getThreadFactory(String name, boolean daemonFlag) {
        return new ThreadFactoryBuilder()
                .setNameFormat(name)
                .setDaemon(daemonFlag)
                .build();
    }
}
