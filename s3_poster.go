package main

import (
	"bytes"
	"fmt"
	"io"
	"sync"
	"time"

	metrics "github.com/rcrowley/go-metrics"
	s3 "github.com/rlmcpherson/s3gof3r"
)

var s3DeliverySizeHistogram = metrics.GetOrRegisterHistogram("lumbermill.poster.s3.deliver.sizes", metrics.DefaultRegistry, metrics.NewUniformSample(100))

type S3Poster struct {
	destination  *Destination
	bucket       *s3.Bucket
	currentFiles []s3File

	pointsSuccessCounter metrics.Counter
	pointsSuccessTime    metrics.Timer
	pointsFailureCounter metrics.Counter
	pointsFailureTime    metrics.Timer
	waitGroup            *sync.WaitGroup
}

type s3File struct {
	fileName string
	writer   io.WriteCloser
}

func NewS3Poster(destination *Destination, bucketName string, waitGroup *sync.WaitGroup) Poster {
	keys, _ := s3.EnvKeys()
	client := s3.New("", keys)

	return &S3Poster{
		destination:  destination,
		bucket:       client.Bucket(bucketName),
		currentFiles: make([]s3File, numSeries),
		waitGroup:    waitGroup,
	}
}

func (p *S3Poster) Run() {
	var last bool
	var delivery [][]Point

	p.waitGroup.Add(1)
	timeout := time.NewTicker(time.Second)
	defer func() { timeout.Stop() }()
	defer p.waitGroup.Done()

	for !last {
		delivery, last = p.nextDelivery(timeout)
		p.deliver(delivery)
	}
}

func (p *S3Poster) nextDelivery(timeout *time.Ticker) (delivery [][]Point, last bool) {
	delivery = make([][]Point, int(numSeries)) // record type -> slice of points.
	for i := 0; i < int(numSeries); i++ {
		delivery[i] = make([]Point, 0)
	}

	for {
		select {
		case point, open := <-p.destination.points:
			if open {
				series := delivery[point.Type]
				series = append(series, point)
				delivery[point.Type] = series
			} else {
				return delivery, true
			}
		case <-timeout.C:
			return delivery, false
		}
	}
}

func (p *S3Poster) deliver(allSeries [][]Point) {
	// Write this data as: series-type/200601021504.tsv
	datePrefix := time.Now().Format("200601021504")

	for seriesType, points := range allSeries {
		if len(points) == 0 {
			continue
		}

		nowFileName := fmt.Sprintf("%s/%s.tsv", SeriesType(seriesType).Name(), datePrefix)
		current := p.currentFiles[seriesType]
		if current.fileName != nowFileName {
			if current.fileName != "" {
				current.writer.Close()
			}

			w, err := p.bucket.PutWriter(nowFileName, nil, nil)
			if err != nil {
				log.Errorf("fn=delivery poster=s3 err=%q", err)
				continue
			} else {
				current = s3File{fileName: nowFileName, writer: w}
				p.currentFiles[seriesType] = current
			}
		}

		r := new(bytes.Buffer)
		for _, pt := range points {
			// Forgive me father, for I have sinned.
			r.WriteString(pt.Token)
			for _, p := range pt.Points {
				r.WriteString(fmt.Sprintf("\t%v", p))
			}
			r.WriteString("\n")
		}
		io.Copy(current.writer, r)
	}
}
