import json
import logging
import threading
import time
from collections import defaultdict

import websocket

from hyperliquid.utils.types import Any, Callable, Dict, List, NamedTuple, Optional, Subscription, Tuple, WsMsg

#ActiveSubscription = NamedTuple("ActiveSubscription", [("callback", Callable[[Any], None]), ("subscription_id", int)])
ActiveSubscription = NamedTuple("ActiveSubscription", [("callback", Callable[[Any], None]), ("callback_thread", threading.Event), ("subscription_id", int)])

#self.channel_callbacks_running: Dict[str, threading.Event] = defaultdict(threading.Event)


class WebSocketConnectionError(Exception):
    pass

def subscription_to_identifier(subscription: Subscription) -> str:
    if subscription["type"] == "allMids":
        return "allMids"
    elif subscription["type"] == "l2Book":
        return f'l2Book:{subscription["coin"].lower()}'
    elif subscription["type"] == "trades":
        return f'trades:{subscription["coin"].lower()}'
    elif subscription["type"] == "userEvents":
        return "userEvents"
    elif subscription["type"] == "orderUpdates":
        return "orderUpdates"
    elif subscription["type"] == "userFills":
        return "userFills"


def ws_msg_to_identifier(ws_msg: WsMsg) -> Optional[str]:
    if ws_msg["channel"] == "pong":
        return "pong"
    elif ws_msg["channel"] == "allMids":
        return "allMids"
    elif ws_msg["channel"] == "l2Book":
        return f'l2Book:{ws_msg["data"]["coin"].lower()}'
    elif ws_msg["channel"] == "trades":
        trades = ws_msg["data"]
        if len(trades) == 0:
            return None
        else:
            return f'trades:{trades[0]["coin"].lower()}'
    elif ws_msg["channel"] == "user":
        return "userEvents"
    elif ws_msg["channel"] == "orderUpdates":
        return "orderUpdates"
    elif ws_msg["channel"] == "userFills":
        return "userFills"


class WebsocketManager(threading.Thread):
    def __init__(self, base_url):
        super().__init__()
        self.subscription_id_counter = 0
        self.ws_ready = False
        self.queued_subscriptions: List[Tuple[Subscription, ActiveSubscription]] = []
        self.active_subscriptions: Dict[str, List[ActiveSubscription]] = defaultdict(list)
        ws_url = "ws" + base_url[len("http") :] + "/ws"
        self.ws = websocket.WebSocketApp(ws_url, on_message=self.on_message, on_open=self.on_open)
        self.ping_sender = threading.Thread(target=self.send_ping)

    def run(self):
        self.ping_sender.start()
        self.ws.run_forever()

    def send_ping(self):
        while True:
            try:
                time.sleep(48)
                logging.debug("Websocket sending ping")
                self.ws.send(json.dumps({"method": "ping"}))
            except:
                raise WebSocketConnectionError

    def on_message(self, _ws, message):
        if message == "Websocket connection established.":
            logging.debug(message)
            return
        logging.debug(f"on_message {message}")
        #logging.info(f"on_message {message}")
        ws_msg: WsMsg = json.loads(message)
        identifier = ws_msg_to_identifier(ws_msg)
        if identifier == "pong":
            logging.debug("Websocket received pong")
            return
        if identifier is None:
            logging.debug("Websocket not handling empty message")
            return
        active_subscriptions = self.active_subscriptions[identifier]
        if len(active_subscriptions) == 0:
            print("Websocket message from an unexpected subscription:", message, identifier)
        else:
            for active_subscription in active_subscriptions:
                #we check that there is no callback already running on this subscription
                #we only block for OB channel
                if "l2Book" in identifier:
                    if not active_subscription.callback_thread.is_set():
                        active_subscription.callback_thread.set()
                        threading.Thread(target=self.handle_message, args=(active_subscription, ws_msg)).start()
                    else:
                        logging.debug(f"Callback {active_subscription.callback.__name__} is already running, discarding msg: {ws_msg}")
                else:
                    self.handle_message(active_subscription, ws_msg)

    def handle_message(self, active_subscription, ws_msg):
        try:
            active_subscription.callback(ws_msg)
        finally:
            active_subscription.callback_thread.clear()

    def on_open(self, _ws):
        logging.debug("on_open")
        self.ws_ready = True
        for subscription, active_subscription in self.queued_subscriptions:
            self.subscribe(subscription, active_subscription.callback, active_subscription.subscription_id)

    def stop(self):
        self.stop_event.set()  # Signal the ping_sender to stop
        self.ws.close()  # Close the WebSocket connection
        self.join()  # Wait for the main thread to finish
        self.ping_sender.join()  # Wait for the ping_sender thread to finish

    def subscribe(
        self, subscription: Subscription, callback: Callable[[Any], None], subscription_id: Optional[int] = None
    ) -> int:
        if subscription_id is None:
            self.subscription_id_counter += 1
            subscription_id = self.subscription_id_counter
        if not self.ws_ready:
            logging.debug("enqueueing subscription")
            self.queued_subscriptions.append((subscription, ActiveSubscription(callback, subscription_id)))
        else:
            logging.debug("subscribing")
            identifier = subscription_to_identifier(subscription)
            #TODO Check if I need to solve this with orderUpdates, it's probably bad to subscribe mutltiple times to the same channel
            if subscription["type"] == "userEvents":
                # TODO: ideally the userEvent messages would include the user so that we can support multiplexing them
                if len(self.active_subscriptions[identifier]) != 0:
                    raise NotImplementedError("Cannot subscribe to UserEvents multiple times")
            self.active_subscriptions[identifier].append(ActiveSubscription(callback, threading.Event(), subscription_id))
            self.ws.send(json.dumps({"method": "subscribe", "subscription": subscription}))
        return subscription_id

    def unsubscribe(self, subscription: Subscription, subscription_id: int) -> bool:
        if not self.ws_ready:
            raise NotImplementedError("Can't unsubscribe before websocket connected")
        identifier = subscription_to_identifier(subscription)
        active_subscriptions = self.active_subscriptions[identifier]
        new_active_subscriptions = [x for x in active_subscriptions if x.subscription_id != subscription_id]
        if len(new_active_subscriptions) == 0:
            self.ws.send(json.dumps({"method": "unsubscribe", "subscription": subscription}))
        self.active_subscriptions[identifier] = new_active_subscriptions
        return len(active_subscriptions) != len(new_active_subscriptions)


