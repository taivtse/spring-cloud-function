package org.springframework.cloud.function.rsocket;

import java.net.InetSocketAddress;
import java.time.Duration;

import org.springframework.lang.Nullable;

import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.rsocket.core.RSocketConnector;
import io.rsocket.core.RSocketServer;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.TcpServerTransport;
import reactor.core.Disposable;
import reactor.util.retry.Retry;
import reactor.util.retry.RetrySpec;

public abstract class RSocketConnectionUtils {

	public static Disposable createServerSocket(RSocket rsocket, InetSocketAddress address) {
		Disposable server = RSocketServer.create(SocketAcceptor.with(rsocket))
		.bind(TcpServerTransport.create(address)) //TODO transport can actually be selected based on address (local or tcp)??
		.subscribe();
		return server;
	}

	public static RSocket createClientSocket(InetSocketAddress address, @Nullable RetrySpec retrySpec) {
		RSocket socket = RSocketConnector.connectWith(TcpClientTransport.create(address)).log()
				.retryWhen(retrySpec == null ? Retry.backoff(5, Duration.ofSeconds(1)) : retrySpec)
				.block();
		return socket;
	}
}
