package main

import (
	"build_app_test/config"
	"build_app_test/worker/queue"
	"build_app_test/worker/worker"
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"
	"github.com/aws/aws-sdk-go-v2/aws"
	aws_config "github.com/aws/aws-sdk-go-v2/config"
	aws_credentials "github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/labstack/echo/v5"
	_ "github.com/lib/pq"
	"github.com/spf13/viper"
	"build_app_test/upload"
)



type deployRequest struct {
	Token   string `json:"token"`
	CloneURL string `json:"cloneURL"`
}

type Deployment struct {
	ID 			string 	   `db:"id"`
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

	
	bucketName := viper.GetString("CLOUDFLARE_BUCKET_NAME")
	accountId := viper.GetString("CLOUDFLARE_ACCOUNT_ID")
	accessKeyId := viper.GetString("CLOUDFLARE_ACCESS_KEY_ID")
	accessKeySecret := viper.GetString("CLOUDFLARE_ACCESS_KEY_SECRET")
	publicBaseURL := viper.GetString("CLOUDFLARE_PUBLIC_BASE_URL")

	cfg,err := aws_config.LoadDefaultConfig(context.TODO(),
		aws_config.WithCredentialsProvider(aws_credentials.NewStaticCredentialsProvider(accessKeyId,accessKeySecret,"")),
		aws_config.WithRegion("auto"),
	)

	if err != nil{
		log.Fatal(err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
      o.BaseEndpoint = aws.String(fmt.Sprintf("https://%s.r2.cloudflarestorage.com", accountId))
  	})

	artifactUploader := upload.NewUpload(client, bucketName, publicBaseURL)

	rmq,err := queue.NewRabbitMQ(viper.GetString("RABBITMQ_CONNECTION_STRING"))

	if err != nil{
		log.Fatal(err)
	}

	defer rmq.Close()

	if err := rmq.SetUpQueues(); err != nil {
		log.Fatal(err)
	}

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

	go func() {
		if err := worker.StartWorker(rmq, db, artifactUploader); err != nil {
			log.Printf("worker stopped: %v", err)
		}
	}()

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
			RETURNING id
		`
		
		ctx := c.Request().Context()

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

		err = db.QueryRowContext(ctx,query,
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
		).Scan(&inst.ID); 
		
		if err != nil {
			return c.JSON(http.StatusInternalServerError, map[string]string{"message": "failed to create deployment"})
		}

		job := queue.DeployJob{DeploymentID: inst.ID, Clone_URL: inst.CloneURL,RetryCount:inst.RetryCount}

		if err := rmq.PublishJob(job); err != nil {
			return c.JSON(500, map[string]string{"error": "failed to queue job"})
		}
		
		return c.JSON(http.StatusOK, map[string]string{"message": "deploy request accepted"})
	})

	log.Fatal(e.Start(":8080"))

}
