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

/**
 *
 * 
 * @author shijia.wxr
 *
 */

/**
 * netty通信配置类
 * @author lvchenggang
 *
 */
public class NettyServerConfig implements Cloneable {
	// server 监听端口
	private int		listenPort							= 8888;
	/**
	 * <pre>
	 *  Netty Server Worker codec线程,用于:
	 *  序列化/反序列化ChannelHandler(对应的线程名: NettyServerCodecThread_i)
	 *  但对于namesrv, 也用于:
	 *  对Request命令进行处理,并生成Response命令(对应的线程名为: RemotingExecutorThread_i)
	 * </pre>
	 */
	private int		serverWorkerThreads					= 8;

	/**
	 * <pre>
	 * Netty Server Worker 命令处理线程,用于:
	 * 对Request命令进行处理,并生成Response命令(对应的线程名为: NettyServerPublicExecutor_i)
	 * 但namesrv没有用这个线程池
	 * </pre>
	 */
	private int		serverCallbackExecutorThreads		= 0;

	/**
	 * <pre>
	 * Netty Server IO线程, 对应线程名NettyServerNIOSelector_i 
	 * 另,Netty Server Boss线程固定为1, 对应线程名NettyBoss_0
	 * </pre>
	 */
	private int		serverSelectorThreads				= 3;

	// Oneway信号量的值
	private int		serverOnewaySemaphoreValue			= 256;

	// 异步信号量的值
	private int		serverAsyncSemaphoreValue			= 64;

	// server心跳检测时间间隔
	private int		serverChannelMaxIdleTimeSeconds		= 120;

	// server端发送缓冲区大小
	private int		serverSocketSndBufSize				= NettySystemConfig.socketSndbufSize;
	// server端接收缓冲区大小
	private int		serverSocketRcvBufSize				= NettySystemConfig.socketRcvbufSize;
	private boolean	serverPooledByteBufAllocatorEnable	= true;

	/**
	 * make make install
	 *
	 *
	 * ../glibc-2.10.1/configure \ --prefix=/usr \ --with-headers=/usr/include \ --host=x86_64-linux-gnu \
	 * --build=x86_64-pc-linux-gnu \ --without-gd
	 */
	private boolean	useEpollNativeSelector				= false;

	public int getListenPort() {
		return listenPort;
	}

	public void setListenPort(int listenPort) {
		this.listenPort = listenPort;
	}

	public int getServerWorkerThreads() {
		return serverWorkerThreads;
	}

	public void setServerWorkerThreads(int serverWorkerThreads) {
		this.serverWorkerThreads = serverWorkerThreads;
	}

	public int getServerSelectorThreads() {
		return serverSelectorThreads;
	}

	public void setServerSelectorThreads(int serverSelectorThreads) {
		this.serverSelectorThreads = serverSelectorThreads;
	}

	public int getServerOnewaySemaphoreValue() {
		return serverOnewaySemaphoreValue;
	}

	public void setServerOnewaySemaphoreValue(int serverOnewaySemaphoreValue) {
		this.serverOnewaySemaphoreValue = serverOnewaySemaphoreValue;
	}

	public int getServerCallbackExecutorThreads() {
		return serverCallbackExecutorThreads;
	}

	public void setServerCallbackExecutorThreads(int serverCallbackExecutorThreads) {
		this.serverCallbackExecutorThreads = serverCallbackExecutorThreads;
	}

	public int getServerAsyncSemaphoreValue() {
		return serverAsyncSemaphoreValue;
	}

	public void setServerAsyncSemaphoreValue(int serverAsyncSemaphoreValue) {
		this.serverAsyncSemaphoreValue = serverAsyncSemaphoreValue;
	}

	public int getServerChannelMaxIdleTimeSeconds() {
		return serverChannelMaxIdleTimeSeconds;
	}

	public void setServerChannelMaxIdleTimeSeconds(int serverChannelMaxIdleTimeSeconds) {
		this.serverChannelMaxIdleTimeSeconds = serverChannelMaxIdleTimeSeconds;
	}

	public int getServerSocketSndBufSize() {
		return serverSocketSndBufSize;
	}

	public void setServerSocketSndBufSize(int serverSocketSndBufSize) {
		this.serverSocketSndBufSize = serverSocketSndBufSize;
	}

	public int getServerSocketRcvBufSize() {
		return serverSocketRcvBufSize;
	}

	public void setServerSocketRcvBufSize(int serverSocketRcvBufSize) {
		this.serverSocketRcvBufSize = serverSocketRcvBufSize;
	}

	public boolean isServerPooledByteBufAllocatorEnable() {
		return serverPooledByteBufAllocatorEnable;
	}

	public void setServerPooledByteBufAllocatorEnable(boolean serverPooledByteBufAllocatorEnable) {
		this.serverPooledByteBufAllocatorEnable = serverPooledByteBufAllocatorEnable;
	}

	public boolean isUseEpollNativeSelector() {
		return useEpollNativeSelector;
	}

	public void setUseEpollNativeSelector(boolean useEpollNativeSelector) {
		this.useEpollNativeSelector = useEpollNativeSelector;
	}

	@Override
	public Object clone() throws CloneNotSupportedException {
		return (NettyServerConfig) super.clone();
	}
}
