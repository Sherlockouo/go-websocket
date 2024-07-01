package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
)

var (
	upgrader = websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
	}
	rdb = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "j5QzIJhJQHOK4Eb",
		DB:       0,
	})
	pingInterval    = 500 * time.Millisecond
	maxPingFailures = 3
)

const (
	// channels
	AnonymousNotificationsChannel = "system:notifications:anonymous"
	NotificationsChannel          = "system:notifications"
	UserChannel                   = "user:%s"

	// events
	AuthEvent        = "system:auth"
	OauthNotionEvent = "oauth:notion"
)

type Client struct {
	conn          *websocket.Conn
	send          chan []byte
	subscriptions map[string]chan struct{}
	done          chan struct{}
	pingFailures  int
	UserID        string
}

type Message struct {
	ID    int64  `json:"id"`    // message id
	Event string `json:"event"` // event
	Data  string `json:"data"`  // raw data
}

func (m *Message) Marshal() []byte {
	res, _ := json.Marshal(m)
	return res
}

// Subscribe to the notifications board channel if not already subscribed
func (c *Client) subscribeChannels(channels ...string) {
	for _, channel := range channels {
		if _, ok := c.subscriptions[channel]; !ok {
			slog.Info("subscribing to", "channel", channel)
			c.Subscribe(channel)
		}
	}
}

func (c *Client) handleMessage(message []byte) error {
	var msg Message
	if err := json.Unmarshal(message, &msg); err != nil {
		slog.Info("unmarshal error:", "err", err)
		return err
	}

	slog.Info("event type ", "event", msg.Event)
	switch msg.Event {
	// websocket 登录，验证 heder 的token
	case AuthEvent:
		// TODO: 解析 jwt token，获取用户 UserID
		// token := msg.Data
		c.UserID = "xxx"
		if msg.Data != "xxx" {
			return errors.New("auth failed")
		}

	default:
		return errors.New("unsupported event")
	}

	// 登录后 删除匿名通知渠道
	if cancel, ok := c.subscriptions[AnonymousNotificationsChannel]; ok {
		cancel <- struct{}{}
		delete(c.subscriptions, AnonymousNotificationsChannel)
	}

	c.subscribeChannels(NotificationsChannel, fmt.Sprintf(UserChannel, c.UserID))
	return nil
}

func (c *Client) readPump() {
	defer func() {
		c.closeSubscriptions()
		c.conn.Close()
	}()
	for {
		select {
		case <-c.done:
			return
		default:
			msgType, message, err := c.conn.ReadMessage()
			if err != nil {
				slog.Info("read error:", "err", err)
				if errors.Is(err, websocket.ErrCloseSent) {
					slog.Info("read error, websocket closed", "UserID", c.UserID, "err", err)
					// 关闭 conn
					c.done <- struct{}{}
					return
				}
				return
			}

			switch msgType {
			case websocket.PingMessage:
				if err := c.sendMessage(websocket.PongMessage, nil); err != nil {
					slog.Error("Handle ping error", "err", err)
				}
			case websocket.TextMessage:
				if err := c.handleMessage(message); err != nil {
					slog.Error("Handle text message error", "err", err)
				}
			case websocket.CloseMessage:
				c.done <- struct{}{}
				slog.Warn("client send close websocket")
			case websocket.BinaryMessage:
				slog.Error("Binary message is not supported", "err", err)

			default:
				slog.Error("unhandled message type:", "type", msgType)
			}
		}
	}
}

// send message
func (c *Client) sendMessage(msgType int, msg []byte) error {
	if err := c.conn.WriteMessage(msgType, msg); err != nil {
		slog.Info("write error:", "err", err)
		return err
	}
	return nil
}

func (c *Client) writePump() {
	ticker := time.NewTicker(pingInterval)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()
	// tick and send message
	for {
		select {
		case msg := <-c.send:
			if err := c.sendMessage(websocket.TextMessage, msg); err != nil {
				slog.Error("send text message error", "err", err)
			}
		case <-ticker.C:
			if err := c.sendMessage(websocket.PingMessage, nil); err != nil {
				if errors.Is(err, websocket.ErrCloseSent) {
					slog.Info("read error, websocket closed", "UserID", c.UserID, "err", err)
					c.done <- struct{}{}
					return
				}
				slog.Error("ping error:", "err", err)
				c.pingFailures++
				if c.pingFailures >= maxPingFailures {
					slog.Info("max ping failures reached, closing connection")
					return
				}
			} else {
				c.pingFailures = 0
			}
		case <-c.done:
			return
		}
	}
}

func (c *Client) Subscribe(channelID string) {
	pubsub := rdb.Subscribe(context.Background(), channelID)
	cancel := make(chan struct{})
	c.subscriptions[channelID] = cancel

	go func() {
		ch := pubsub.Channel()
		for {
			select {
			case msg := <-ch:
				c.send <- []byte(msg.Payload)
			case <-cancel:
				pubsub.Close()
				return
			}
		}
	}()
}

func (c *Client) closeSubscriptions() {
	for _, cancel := range c.subscriptions {
		cancel <- struct{}{}
	}
}

func serveWs(w http.ResponseWriter, r *http.Request) {
	upgrader.CheckOrigin = func(r *http.Request) bool { return true }
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		slog.Error("upgrade error:", "err", err)
		return
	}
	client := &Client{
		conn:          conn,
		send:          make(chan []byte, 256),
		subscriptions: make(map[string]chan struct{}),
	}

	// 未登录,或者未订阅过 匿名通知通道
	if _, ok := client.subscriptions[AnonymousNotificationsChannel]; client.UserID == "" || !ok {
		client.Subscribe(AnonymousNotificationsChannel)
	}
	go client.readPump()
	go client.writePump()
}

func publishMessage(w http.ResponseWriter, r *http.Request) {
	var msg Message
	if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
		http.Error(w, "Invalid message format", http.StatusBadRequest)
		return
	}
	res, err := rdb.Publish(context.Background(), NotificationsChannel, msg.Marshal()).Result()
	if err != nil {
		slog.Info("publish error:", "err", err)
		http.Error(w, "Failed to publish message", http.StatusInternalServerError)
		return
	}
	slog.Info("Published message", "res", res)
	w.Write([]byte("Message published to channel: " + NotificationsChannel))
}

func publishAnonymousMessage(w http.ResponseWriter, r *http.Request) {
	var msg Message
	if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
		http.Error(w, "Invalid message format", http.StatusBadRequest)
		return
	}
	res, err := rdb.Publish(context.Background(), AnonymousNotificationsChannel, msg.Marshal()).Result()
	if err != nil {
		slog.Info("publish error:", "err", err)
		http.Error(w, "Failed to publish message", http.StatusInternalServerError)
		return
	}
	slog.Info("Published message", "res", res)
	w.Write([]byte("Message published to channel: " + NotificationsChannel))
}

func main() {
	http.HandleFunc("/ws", serveWs)
	http.HandleFunc("/publish", publishMessage)
	http.HandleFunc("/publish_anonymous", publishAnonymousMessage)
	slog.Info("Server started on :8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		slog.Error("ListenAndServe:", "err", err)
	}
}
