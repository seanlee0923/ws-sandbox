package main

import (
	"fmt"
	"github.com/gorilla/websocket"
	"log"
	"net/http"
	"os"
)

func main() {
	// dialer 우선 만들고
	dialer := websocket.DefaultDialer

	// 클라이언트 3개
	for i := range 3 {
		// 3개 병렬
		go func() {

			c, resp, err := dialer.Dial(fmt.Sprintf("ws://localhost:9090/ws/%d", i), http.Header{})
			if err != nil {
				log.Println(err)
				os.Exit(1)
			}

			log.Print(resp)

			// read pump
			go func() {

				for {
					_, msg, err := c.ReadMessage()
					if err != nil {
						panic(err)
					}
					// 로그출력
					log.Println(i, string(msg))

				}
			}()

			err = c.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf("hi i am %d", i)))
			if err != nil {
				log.Println(err)
			}

		}()
	}

	// 대기해야함
	select {}

}
