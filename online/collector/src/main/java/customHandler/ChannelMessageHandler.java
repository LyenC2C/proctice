package customHandler;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.flume.Event;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.event.EventBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by qf on 16/4/25.
 */
public class ChannelMessageHandler extends ChannelInboundHandlerAdapter {
    private static final Logger logger = LoggerFactory
            .getLogger(ChannelMessageHandler.class);

    private ChannelProcessor channelProcessor;

    public void setChannelProcessor(ChannelProcessor channelProcessor) {
        this.channelProcessor = channelProcessor;
    }

    public ChannelProcessor getChannelProcessor() {
        return channelProcessor;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        /*
        ByteBuf bytebuf = (ByteBuf) msg;
        byte[] bytes = new byte[bytebuf.readableBytes()];
        bytebuf.readBytes(bytes);
        List<Event> eventList = new ArrayList<>();
        eventList.add(EventBuilder.withBody(bytes));
        channelProcessor.processEventBatch(eventList);
        */
        /*
        logger.info(String.valueOf(msg));
        ctx.writeAndFlush(msg.toString());
        */
        List<Event> eventList = new ArrayList<>();
        Map<String, String> header = new HashMap<>();
        String ts = String.valueOf(System.currentTimeMillis());
        header.put("timestamp", ts);
        eventList.add(EventBuilder.withBody(String.valueOf(msg), Charset.forName("utf-8"), header));
        logger.info("Puting data into flume channel ...");
        channelProcessor.processEventBatch(eventList);
    }


    public void channelReadComplete(ChannelHandlerContext ctx) {

        ctx.flush();
    }
}
