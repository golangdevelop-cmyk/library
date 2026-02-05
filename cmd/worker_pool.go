package main

import (
	"context"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	concurrent "github.com/golangdevelop-cmyk/library/internal/concurrent/workerpool"
)

type Job struct {
	url  string
	file *os.File
}

func images(count int) <-chan string {
	ch := make(chan string)

	go func() {
		defer close(ch)
		for range count {
			ch <- "https://picsum.photos/200"
		}
	}()

	return ch
}

func download(url string, file *os.File) error {
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	defer file.Close()
	_, err = io.Copy(file, resp.Body)
	if err != nil {
		return err
	}
	log.Printf("File download: %s\n", file.Name())
	return nil
}

func Main() {
	start := time.Now()

	imagesCount := 100

	tempDir, err := os.MkdirTemp(os.TempDir(), "*")
	if err != nil {
		log.Fatal(err)
	}

	cnt, cnl := context.WithTimeout(context.Background(), time.Second*5)
	defer cnl()

	// канал для передачи входных данных горутинам
	jobs := make(chan Job)

	go func() {
		defer close(jobs)
		for url := range images(imagesCount) {
			file, err := os.CreateTemp(tempDir, "image.*.png")
			if err != nil {
				//results <- concurrent.NewResult(err) TODO
			} else {
				jobs <- Job{
					url:  url,
					file: file,
				}
			}
		}
	}()

	pool := concurrent.NewWorkerPool(5, cnt, jobs, func(job Job) error {
		return download(job.url, job.file)
	})

	// собираем результаты
	for res := range pool.Execute() {
		if res.Error() == nil {
			log.Printf("Job finish")
		} else {
			log.Printf("Job error %s", res.Error().Error())
		}
	}

	elapsed := time.Since(start)
	log.Printf("Total time took %s", elapsed)
}
