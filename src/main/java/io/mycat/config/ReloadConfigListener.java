package io.mycat.config;

import io.mycat.manager.response.ReloadConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.*;

public class ReloadConfigListener implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger("ReloadConfigLister");

    public Path path;

    public ReloadConfigListener() {
        try {
            path = Paths.get(getClass().getClassLoader().getResource("").toURI());
        } catch (Exception e) {
            LOGGER.error("获取配置文件路径失败");
        }
        LOGGER.info("加载自动读取配置文件成功");

        new Thread(this).start();
    }

    @Override
    public void run() {
        try (WatchService watchService = FileSystems.getDefault().newWatchService()) {
            path.register(watchService, StandardWatchEventKinds.ENTRY_MODIFY);
            for (; ; ) {
                final WatchKey key = watchService.take();
                for (WatchEvent<?> watchEvent : key.pollEvents()) {
                    final WatchEvent.Kind<?> kind = watchEvent.kind();
                    //修改事件
                    if (kind == StandardWatchEventKinds.ENTRY_MODIFY) {
                        ReloadConfig.reload_all();
                        System.out.println("重新加载配置文件");
                    }
                }
                boolean valid = key.reset();
                if (!valid) {
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
