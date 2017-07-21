package customSource;

import customHandler.ChannelMessageHandler;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;
import org.apache.flume.Context;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.Configurables;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by lyen on 17-6-2.
 */
public class NettySource extends AbstractSource implements EventDrivenSource, Configurable {
    private static final Logger logger = LoggerFactory
            .getLogger(NettySource.class);

    private int port;
    private String host = null;

    /**
     * 获取配置
     *
     * @param context
     */
    @Override
    public void configure(Context context) {
        Configurables.ensureRequiredNonNull(context, "port");
        port = context.getInteger("port");
        host = context.getString("host");
    }


    /**
     * 开始收集数据
     */
    @Override
    public synchronized void start() {
        logger.info("Netty tcp custom source start ...");
//         Configure the server.
        EventLoopGroup bossGroup = new NioEventLoopGroup(2);
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        ServerBootstrap b = new ServerBootstrap();
        try {
            ChannelMessageHandler channelMessageHandler = new ChannelMessageHandler();
            channelMessageHandler.setChannelProcessor(getChannelProcessor());
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) throws Exception {
                            ChannelPipeline p = ch.pipeline();
                            p.addLast(
                                    new ObjectEncoder(),
                                    new ObjectDecoder(ClassResolvers.cacheDisabled(null)));
                            p.addLast("messageParseHandler", channelMessageHandler);
                        }
                    });

            // Start the server.
            try {
                ChannelFuture f = b.bind(port).sync();
                // Wait until the server socket is closed.
                f.channel().closeFuture().sync();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        } finally {
            // Shut down all event loops to terminate all threads.
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
        super.start();

    }

    @Override
    public synchronized ChannelProcessor getChannelProcessor() {
        return super.getChannelProcessor();
    }

    /**
     * 结束收集数据
     */
    @Override
    public synchronized void stop() {
        logger.info("Netty tcp sudtom source stop ...");
        super.stop();
    }

}
