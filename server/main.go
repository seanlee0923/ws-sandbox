package main

import (
	"fmt"
	"github.com/gorilla/websocket"
	"log"
	"net/http"
	"strings"
)

// 클라이언트 구조체
type Client struct {
	ID     string
	Conn   *websocket.Conn
	SendCh chan []byte
	Room   *Room
}

// 룸
type Room struct {
	ID string
	// 클라이언트 있는지 확인하는 채널
	Clients     map[*Client]bool
	BroadCastCh chan []byte
	ExitCh      chan *Client
}

// 아이디 이거로 뽑기
var i int
var clients map[string]*Client

// 고루틴끼리 이거로 넘기자
var cliCH chan *Client
var delCh chan string

// 엔트리포인트
func main() {
	// 클라이언트 관리용 맵
	clients = make(map[string]*Client)
	// 클라이언트 관리용 채널 init
	cliCH = make(chan *Client)
	delCh = make(chan string)

	// 룸 관리용 함수
	go connectionManager(cliCH, delCh)

	http.HandleFunc("/ws/", upgrade)
	http.ListenAndServe(":9090", nil)

}

// 업그레이드 시키기
func upgrade(w http.ResponseWriter, r *http.Request) {
	idSplit := strings.Split(r.URL.Path, "/")
	id := idSplit[len(idSplit)-1]

	// id 비어있으면 거름
	if id == "" {
		w.WriteHeader(401)
		log.Print("id required")
		return
	}

	// 중복접속 막기
	if _, ok := clients[id]; ok {
		w.WriteHeader(401)
		log.Print("aleady exist")
		return
	}

	// 걍 업그레이더 포인터 만들면서 업글하기
	conn, err := new(websocket.Upgrader).Upgrade(w, r, nil)
	if err != nil {
		w.WriteHeader(500)
		log.Print(err)
		return
	}

	c := Client{
		ID:     id,
		Conn:   conn,
		SendCh: make(chan []byte),
	}
	i++

	// 웹소켓 read, write 시키기
	go c.readPump()
	go c.writePump()

	// 클라이언트 맵에 넣기
	clients[id] = &c

	// 채널에도 전달
	cliCH <- &c
}

// 기본 read pump
func (c *Client) readPump() {
	defer c.Conn.Close()
	defer func() {
		if c.Room != nil {
			c.Room.ExitCh <- c
		}
		delCh <- c.ID
	}()

	for {

		_, msg, err := c.Conn.ReadMessage()
		if err != nil {
			break
		}

		log.Print(c.ID, msg)

		// 만약 룸 존재하면
		if c.Room != nil {
			c.Room.BroadCastCh <- []byte(c.ID + " hi to everyone!")
		}
	}
}

// 기본 write pump
func (c *Client) writePump() {
	defer c.Conn.Close()

	for m := range c.SendCh {
		err := c.Conn.WriteMessage(websocket.TextMessage, m)
		if err != nil {
			break
		}
	}
}

// 방 만들기
func NewRoom(id string) *Room {
	return &Room{
		ID:          id,
		Clients:     make(map[*Client]bool),
		BroadCastCh: make(chan []byte),
		ExitCh:      make(chan *Client),
	}
}

// room 시작시키기
func (r *Room) begin() {
	log.Printf("방 추가: %s", r.ID)

	for {
		select {
		case client := <-r.ExitCh:
			// 클라이언트 나가기
			if _, ok := r.Clients[client]; ok {
				delete(r.Clients, client)
				close(client.SendCh)
				log.Printf("클라이언트 퇴장 (방: %s). 남은 인원: %d", r.ID, len(r.Clients))
			}

		case message := <-r.BroadCastCh:
			// 브로드캐스팅 채널
			for client := range r.Clients {
				select {
				case client.SendCh <- message:
				default:
					close(client.SendCh)
					delete(r.Clients, client)
				}
			}
		}
	}
}

// 커넥션 관리하는 함수
func connectionManager(cliCH chan *Client, delCh chan string) {
	// 대기용 큐
	queue := make([]*Client, 0)
	for {
		select {
		case c := <-cliCH:
			// 사용자 받으면 큐에 넣자
			queue = append(queue, c)

			// 클라이언트 3개 생성마다
			if len(clients)%3 == 0 {
				r := NewRoom(fmt.Sprintf("ROOM-%d", len(clients)))
				// 대기열에 있는애들 룸에 넣기
				for _, cli := range queue {
					r.Clients[cli] = true
				}
				c.Room = r

				go r.begin()
				r.BroadCastCh <- []byte("welcome to room " + r.ID)
				// 대기열 다시 비우기
				queue = make([]*Client, 0)
			}

		case d := <-delCh:
			delete(clients, d)
		}
	}

}
