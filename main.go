package main

import (
	"database/sql"
	"time"
	"fmt"
	"log"
	"net/http"
	"strings"
	"build_app_test/config"
	"github.com/labstack/echo/v5"
	_ "github.com/lib/pq"
	"github.com/spf13/viper"
)



type deployRequest struct {
	Token   string `json:"token"`
	CloneURL string `json:"cloneURL"`
}
type Deployment struct {
	CloneURL    string     `db:"clone_url" json:"clone_url"`
	Status      string     `db:"status" json:"status"`
	RetryCount  int        `db:"retry_count" json:"retry_count"`
	ErrorMessage *string   `db:"error_message" json:"error_message,omitempty"`
	ImageTag    *string    `db:"image_tag" json:"image_tag,omitempty"`
	OutputURL   *string    `db:"output_url" json:"output_url,omitempty"`
	CreatedAt   time.Time  `db:"created_at" json:"created_at"`
	UpdatedAt   time.Time  `db:"updated_at" json:"updated_at"`
	StartedAt   *time.Time `db:"started_at" json:"started_at,omitempty"`
	FinishedAt  *time.Time `db:"finished_at" json:"finished_at,omitempty"`
}
func main(){
	config.LoadConfig()

	dbHost := viper.GetString("DATABASE_HOST")
	dbPort := viper.GetString("DATABASE_PORT")
	dbUser := viper.GetString("DATABASE_USER")
	dbPass := viper.GetString("DATABASE_PASS")
	dbName := viper.GetString("DATABASE_NAME")

	dsn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		dbHost,dbPort,dbUser,dbPass,dbName)
	
	db,err := sql.Open("postgres",dsn)

	if err != nil{
		log.Fatal(err)
	}

	err = db.Ping()
	if err != nil{
		log.Fatal(err)
	}
	
	defer func(){
		err := db.Close()
		if err != nil{
			log.Fatal(err)
		}
	}()

	if(err != nil){
		log.Fatal(err)
	}

	e := echo.New()

	e.POST("/deploy", func(c *echo.Context) error {
		var req deployRequest
		if err := c.Bind(&req); err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{"message": "invalid request body"})
		}

		if strings.TrimSpace(req.Token) == "" || strings.TrimSpace(req.CloneURL) == "" {
			return c.JSON(http.StatusBadRequest, map[string]string{"message": "no token or cloneURL found"})
		}
		
		query := `
			INSERT INTO deployments (
				clone_url,
				status,
				retry_count,
				error_message,
				image_tag,
				output_url,
				created_at,
				updated_at,
				started_at,
				finished_at
			)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		`
		
		ctx := c.Request().Context()

		stmt,err := db.PrepareContext(ctx,query)

		if err!=nil{
			log.Fatal(err)
		}

		now := time.Now()
		inst := Deployment{
			CloneURL:    req.CloneURL,
			Status:      "queued",
			RetryCount:  0,
			ErrorMessage: nil,
			ImageTag:    nil,
			OutputURL:   nil,
			CreatedAt:   now,
			UpdatedAt:   now,
			StartedAt:   nil,
			FinishedAt:  nil,
		}

		if _,err:=stmt.Exec(
			inst.CloneURL,
			inst.Status,
			inst.RetryCount,
			inst.ErrorMessage,
			inst.ImageTag,
			inst.OutputURL,
			inst.CreatedAt,
			inst.UpdatedAt,
			inst.StartedAt,
			inst.FinishedAt,
		); err != nil{
			log.Fatal(err)
		}

		if err := stmt.Close(); err != nil {
			log.Fatal(err)
		}
		
		return c.JSON(http.StatusOK, map[string]string{"message": "deploy request accepted"})
	})

	log.Fatal(e.Start(":8080"))

}
