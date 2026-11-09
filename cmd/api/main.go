package main

import (
	"log"

	"github.com/konlanbuilds/Gofari/internal/config"
)

func main() {
	if _, err := config.Load(); err != nil {
		log.Fatal(err)
	}
	log.Println("gofari api entry point")
}
