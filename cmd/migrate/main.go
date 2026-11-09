package main

import (
	"database/sql"
	"log"
	"os"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/konlanbuilds/Gofari/internal/config"
	"github.com/konlanbuilds/Gofari/migrations"
	"github.com/pressly/goose/v3"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatal(err)
	}

	db, err := sql.Open("pgx", cfg.DatabaseURL)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	goose.SetBaseFS(migrations.FS)
	if err := goose.SetDialect("postgres"); err != nil {
		log.Fatal(err)
	}

	command := "up"
	if len(os.Args) > 1 {
		command = os.Args[1]
	}
	switch command {
	case "up":
		err = goose.Up(db, ".")
	case "status":
		err = goose.Status(db, ".")
	case "down":
		err = goose.Down(db, ".")
	default:
		log.Fatalf("usage: migrate [up|status|down]")
	}
	if err != nil {
		log.Fatal(err)
	}
}
