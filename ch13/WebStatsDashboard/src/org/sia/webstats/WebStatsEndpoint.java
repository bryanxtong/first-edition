package org.sia.webstats;

import java.io.IOException;

import jakarta.websocket.CloseReason;
import jakarta.websocket.EndpointConfig;
import jakarta.websocket.OnClose;
import jakarta.websocket.OnError;
import jakarta.websocket.OnOpen;
import jakarta.websocket.Session;
import jakarta.websocket.server.ServerEndpoint;

/**
 * WebSockets endpoint for receiving messages from LogStatsReceiver and
 * dispatching them to clients.
 *
 */
@ServerEndpoint(value = "/WebStatsEndpoint")
public class WebStatsEndpoint implements LogStatsObserver
{
	private Session currentSession = null;

	@OnOpen
	public void onOpen(Session session, EndpointConfig ec) {
		System.out.println("WebStatsEndpoint onOpen");
		currentSession = session;
		LogStatsReceiver.addObserver(this);
	}

	@OnClose
	public void onClose(Session session, CloseReason reason) {
		System.out.println("WebStatsEndpoint onClose");
		LogStatsReceiver.removeObserver(this);
		currentSession = null;
	}

	@OnError
	public void onError(Throwable t) {
		t.printStackTrace();
	}

	@Override
	public void onStatsMessage(String message)
	{
		sendMessage(message);
	}

	public void sendMessage(String message){
		if(currentSession != null && currentSession.isOpen())
		{
			try {
				currentSession.getBasicRemote().sendText(message);
			} catch (IOException ioe){
				ioe.printStackTrace();
			}
		}
	}
}
