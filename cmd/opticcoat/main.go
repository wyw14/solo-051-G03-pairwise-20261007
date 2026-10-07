package main

import (
	"context"
	"github.com/wyw14/cry-161/internal/api"
	"github.com/wyw14/cry-161/internal/service"
	"log"
	"net/http"
	"os"
)

func main() {
	port := os.Getenv("OPTICCOAT_PORT")
	if port == "" {
		port = "21261"
	}
	svc := service.New()
	svc.StartExecutor(context.Background())
	handler := api.NewRouter(svc)
	log.Printf("opticcoat listening on %s", port)
	if err := http.ListenAndServe("127.0.0.1:"+port, handler); err != nil {
		log.Fatal(err)
	}
}
