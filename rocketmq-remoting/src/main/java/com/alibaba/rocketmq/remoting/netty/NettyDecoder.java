/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.alibaba.rocketmq.remoting.netty;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.remoting.common.RemotingHelper;
import com.alibaba.rocketmq.remoting.common.RemotingUtil;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

/**
 * @author shijia.wxr
 *
 */

/**
 * Netty消息解码器
 * 
 * <pre>
 * 消息格式:
 * |<-4 byte->|<-----4 byte---->|<-------------->|<----->|
 * -------------------------------------------------------
 * |  length  |  header length  |  header data  |  body  |
 * -------------------------------------------------------
 * 
 * </pre>
 * 
 * @author lvchenggang
 *
 */
public class NettyDecoder extends LengthFieldBasedFrameDecoder {
	private static final Logger log = LoggerFactory.getLogger(RemotingHelper.RemotingLogName);
	// 消息大小最大为16m
	private static final int FRAME_MAX_LENGTH = Integer
			.parseInt(System.getProperty("com.rocketmq.remoting.frameMaxLength", "16777216"));

	public NettyDecoder() {
		/**
		 * 目的是去掉消息总长度
		 */
		super(/* maxFrameLength */FRAME_MAX_LENGTH, /* lengthFieldOffset */0, /* lengthFieldLength */4,
				/* lengthAdjustment */0, /* initialBytesToStrip(去掉4个字节的消息长度) */4);
	}

	@Override
	public Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
		ByteBuf frame = null;
		try {
			frame = (ByteBuf) super.decode(ctx, in);
			if (null == frame) {
				return null;
			}

			ByteBuffer byteBuffer = frame.nioBuffer();

			return RemotingCommand.decode(byteBuffer);
		} catch (Exception e) {
			log.error("decode exception, " + RemotingHelper.parseChannelRemoteAddr(ctx.channel()), e);
			RemotingUtil.closeChannel(ctx.channel());
		} finally {
			if (null != frame) {
				frame.release();
			}
		}

		return null;
	}
}
