import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by lyen on 17-6-2.
 */
public class DataClientHandler extends ChannelInboundHandlerAdapter {

    private Logger logger = LoggerFactory.getLogger(DataClientHandler.class);
    private Datas data;

    /**
     * Creates a client-side handler.
     */
    public DataClientHandler(Datas data) {
        this.data = data;
    }


    @Override
    public void channelActive(ChannelHandlerContext ctx) {

        while(true){
            ctx.writeAndFlush(data.getData().toString());
            ctx.writeAndFlush(data.getData().toString());
            ctx.writeAndFlush(data.getData().toString());
            ctx.writeAndFlush(data.getData().toString());
            ctx.writeAndFlush(data.getData().toString());
            logger.info("sending data ...");
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }


    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        System.out.println(msg);
    }


    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }


    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // Close the connection when an exception is raised.
        cause.printStackTrace();
        ctx.close();
    }

}
