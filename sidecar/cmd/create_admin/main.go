package main

import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pg-sage/sidecar/internal/auth"
)

func main() {
	ctx := context.Background()
	dsn := "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer pool.Close()
	id, err := auth.CreateUser(
		ctx, pool, "admin@pg-sage.local", "admin123", "admin",
	)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf(
		"Created admin user id=%d\n"+
			"  email: admin@pg-sage.local\n"+
			"  password: admin123\n"+
			"  URL: http://localhost:8080\n", id,
	)
}
