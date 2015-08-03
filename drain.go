package main

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/bmizerany/lpx"
	"github.com/kr/logfmt"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	// TokenPrefix contains the prefix for non-heroku tokens.
	TokenPrefix = []byte("t.")
	// Heroku contains the prefix for heroku tokens.
	Heroku = []byte("heroku")

	// go-metrics Instruments
	wrongMethodErrorCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace:   "lumbermill",
		Name:        "errors",
		Help:        "x",
		ConstLabels: prometheus.Labels{"error": "drain_wrong_method"},
	})
	authFailureCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace:   "lumbermill",
		Name:        "errors",
		Help:        "x",
		ConstLabels: prometheus.Labels{"error": "auth_failure"},
	})
	badRequestCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace:   "lumbermill",
		Name:        "errors",
		Help:        "x",
		ConstLabels: prometheus.Labels{"error": "badrequest"},
	})
	internalServerErrorCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace:   "lumbermill",
		Name:        "errors",
		Help:        "x",
		ConstLabels: prometheus.Labels{"error": "internalserver"},
	})
	tokenMissingCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace:   "lumbermill",
		Name:        "errors",
		Help:        "x",
		ConstLabels: prometheus.Labels{"error": "token_missing"},
	})
	timeParsingErrorCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace:   "lumbermill",
		Name:        "errors",
		Help:        "x",
		ConstLabels: prometheus.Labels{"error": "time.parse"},
	})
	logfmtParsingErrorCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace:   "lumbermill",
		Name:        "errors",
		Help:        "x",
		ConstLabels: prometheus.Labels{"error": "logfmt.parse"},
	})
	droppedErrorCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace:   "lumbermill",
		Name:        "errors",
		Help:        "x",
		ConstLabels: prometheus.Labels{"error": "dropped"},
	})

	batchCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "lumbermill",
		Name:      "batch",
		Help:      "x",
		//ConstLabels: prometheus.Labels{"error": "internalserver"},
	})

	linesCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "lumbermill",
		Name:      "lines_count",
		Help:      "x",
	})
	routerErrorLinesCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace:   "lumbermill",
		Name:        "lines_router",
		Help:        "x",
		ConstLabels: prometheus.Labels{"router": "error"},
	})
	routerLinesCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace:   "lumbermill",
		Name:        "lines_router",
		Help:        "x",
		ConstLabels: prometheus.Labels{"router": "ok"},
	})
	routerBlankLinesCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace:   "lumbermill",
		Name:        "lines_router",
		Help:        "x",
		ConstLabels: prometheus.Labels{"router": "blank"},
	})
	dynoErrorLinesCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace:   "lumbermill",
		Name:        "lines_dyno",
		Help:        "x",
		ConstLabels: prometheus.Labels{"dyno": "error"},
	})
	dynoMemLinesCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace:   "lumbermill",
		Name:        "lines_dyno",
		Help:        "x",
		ConstLabels: prometheus.Labels{"dyno": "mem"},
	})
	dynoLoadLinesCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace:   "lumbermill",
		Name:        "lines_dyno",
		Help:        "x",
		ConstLabels: prometheus.Labels{"dyno": "load"},
	})
	unknownHerokuLinesCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace:   "lumbermill",
		Name:        "lines_unknown",
		Help:        "x",
		ConstLabels: prometheus.Labels{"unknown": "heroku"},
	})
	unknownUserLinesCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace:   "lumbermill",
		Name:        "lines_unknown",
		Help:        "x",
		ConstLabels: prometheus.Labels{"unknown": "user"},
	})
	parseTimer = prometheus.NewSummary(prometheus.SummaryOpts{
		Namespace: "lumbermill",
		Name:      "parse_time_seconds",
		Help:      "x",
	})
	batchSizeHistogram = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "lumbermill",
		Name:      "batch_size_lines",
		Help:      "x",
	})

	// Capture actual data
	routerService = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Name: "heroku_router_service_ms",
		Help: "Milliseconds responsetime",
	}, []string{
		"id",
		"dyno",
		"method",
		"status",
	})
	routerServiceError = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "heroku_router_error_count",
		Help: "Number of router errors",
	}, []string{
		"id",
		"dyno",
		"method",
		"hcode",
	})
	dynoRuntimeMemSize = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "heroku_runtime_memory_mb",
		Help: "Heroku memory use",
	}, []string{
		"id",
		"dyno",
		"type",
	})
	dynoRuntimeMemPages = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "heroku_runtime_memory_pages",
		Help: "Heroku memory use",
	}, []string{
		"id",
		"dyno",
		"dir",
	})
	dynoRuntimeLoad = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "heroku_runtime_load",
		Help: "Heroku memory use",
	}, []string{
		"id",
		"dyno",
		"span",
	})
)

func init() {
	prometheus.MustRegister(wrongMethodErrorCounter)
	prometheus.MustRegister(authFailureCounter)
	prometheus.MustRegister(badRequestCounter)
	prometheus.MustRegister(internalServerErrorCounter)
	prometheus.MustRegister(tokenMissingCounter)
	prometheus.MustRegister(timeParsingErrorCounter)
	prometheus.MustRegister(logfmtParsingErrorCounter)
	prometheus.MustRegister(droppedErrorCounter)

	prometheus.MustRegister(batchCounter)

	prometheus.MustRegister(linesCounter)
	prometheus.MustRegister(routerErrorLinesCounter)
	prometheus.MustRegister(routerLinesCounter)
	prometheus.MustRegister(dynoErrorLinesCounter)
	prometheus.MustRegister(dynoMemLinesCounter)
	prometheus.MustRegister(dynoLoadLinesCounter)
	prometheus.MustRegister(unknownHerokuLinesCounter)
	prometheus.MustRegister(unknownUserLinesCounter)
	prometheus.MustRegister(parseTimer)
	prometheus.MustRegister(batchSizeHistogram)

	// Actual data-capture
	prometheus.MustRegister(routerService)
	prometheus.MustRegister(routerServiceError)
	prometheus.MustRegister(dynoRuntimeMemSize)
	prometheus.MustRegister(dynoRuntimeMemPages)
	prometheus.MustRegister(dynoRuntimeLoad)
}

// Dyno's are generally reported as "<type>.<#>"
// Extract the <type> and return it
func dynoType(what string) string {
	s := strings.Split(what, ".")
	return s[0]
}

// Lock, or don't do any work, but don't block.
// This, essentially, samples the incoming tokens for the purposes of health checking
// live tokens. Rather than use a random number generator, or a global counter, we
// let the scheduler do the sampling for us.
func (s *server) maybeUpdateRecentTokens(host, id string) {
	if atomic.CompareAndSwapInt32(s.tokenLock, 0, 1) {
		s.recentTokensLock.Lock()
		s.recentTokens[host] = id
		s.recentTokensLock.Unlock()
		atomic.StoreInt32(s.tokenLock, 0)
	}
}

func handleLogFmtParsingError(msg []byte, err error) {
	logfmtParsingErrorCounter.Inc()
	log.Printf("logfmt unmarshal error(%q): %q\n", string(msg), err)
}

// "Parse tree" from hell
func (s *server) serveDrain(w http.ResponseWriter, r *http.Request) {
	s.Add(1)
	defer s.Done()

	w.Header().Set("Content-Length", "0")

	if r.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		wrongMethodErrorCounter.Inc()
		return
	}

	id := r.Header.Get("Logplex-Drain-Token")

	batchCounter.Inc()

	parseStart := time.Now()
	lp := lpx.NewReader(bufio.NewReader(r.Body))

	linesCounterInc := 0

	// HACK: Print out headers && query params
	fmt.Printf("URL: %#v\n", r.URL);
	fmt.Printf("Headers: %#v\n", r.Header);

	for lp.Next() {
		linesCounterInc++
		header := lp.Header()

		// If the syslog Name Header field contains what looks like a log token,
		// let's assume it's an override of the id and we're getting the data from the magic
		// channel
		if bytes.HasPrefix(header.Name, TokenPrefix) {
			id = string(header.Name)
		}

		// If we still don't have an id, throw an error and try the next line
		if id == "" {
			tokenMissingCounter.Inc()
			continue
		}

		destination := s.hashRing.Get(id)

		msg := lp.Bytes()
		switch {
		case bytes.Equal(header.Name, Heroku), bytes.HasPrefix(header.Name, TokenPrefix):
			timeStr := string(lp.Header().Time)
			t, e := time.Parse("2006-01-02T15:04:05.000000+00:00", timeStr)
			if e != nil {
				t, e = time.Parse("2006-01-02T15:04:05+00:00", timeStr)
				if e != nil {
					timeParsingErrorCounter.Inc()
					log.Printf("Error Parsing Time(%s): %q\n", string(lp.Header().Time), e)
					continue
				}
			}

			timestamp := t.UnixNano() / int64(time.Microsecond)

			pid := string(header.Procid)
			switch pid {
			case "router":
				// Decode the raw line no matter what
				rm := routerMsg{}
				err := logfmt.Unmarshal(msg, &rm)
				if err != nil {
					handleLogFmtParsingError(msg, err)
					continue
				}

				switch {
				// router logs with a H error code in them
				case bytes.Contains(msg, keyCodeH):
					routerErrorLinesCounter.Inc()
					re := routerError{}
					err := logfmt.Unmarshal(msg, &re)
					if err != nil {
						handleLogFmtParsingError(msg, err)
						continue
					}
					destination.PostPoint(point{id, routerEvent, []interface{}{timestamp, re.Code}})

					routerServiceError.WithLabelValues(id, rm.Dyno, rm.Method, fmt.Sprint(re.Code)).Inc()

				// If the app is blank (not pushed) we don't care
				// do nothing atm, increment a counter
				case bytes.Contains(msg, keyCodeBlank), bytes.Contains(msg, keyDescBlank):
					routerBlankLinesCounter.Inc()

				// likely a standard router log
				default:
					routerLinesCounter.Inc()
					destination.PostPoint(point{id, routerRequest, []interface{}{timestamp, rm.Status, rm.Service}})

					routerService.WithLabelValues(id, rm.Dyno, rm.Method, fmt.Sprint(rm.Status)).Observe(float64(rm.Service))
				}

				// Non router logs, so either dynos, runtime, etc
			default:
				switch {
				// Dyno error messages
				case bytes.HasPrefix(msg, dynoErrorSentinel):
					dynoErrorLinesCounter.Inc()
					de, err := parseBytesToDynoError(msg)
					if err != nil {
						handleLogFmtParsingError(msg, err)
						continue
					}

					what := string(lp.Header().Procid)
					destination.PostPoint(
						point{id, dynoEvents, []interface{}{timestamp, what, "R", de.Code, string(msg), dynoType(what)}},
					)

				// Dyno log-runtime-metrics memory messages
				case bytes.Contains(msg, dynoMemMsgSentinel):
					s.maybeUpdateRecentTokens(destination.Name, id)

					dynoMemLinesCounter.Inc()
					dm := dynoMemMsg{}
					err := logfmt.Unmarshal(msg, &dm)
					if err != nil {
						handleLogFmtParsingError(msg, err)
						continue
					}
					if dm.Source != "" {
						destination.PostPoint(
							point{
								id,
								dynoMem,
								[]interface{}{
									timestamp,
									dm.Source,
									dm.MemoryCache,
									dm.MemoryPgpgin,
									dm.MemoryPgpgout,
									dm.MemoryRSS,
									dm.MemorySwap,
									dm.MemoryTotal,
									dynoType(dm.Source),
								},
							},
						)

						dynoRuntimeMemSize.WithLabelValues(id, dm.Source, "cache").Set(dm.MemoryCache)
						dynoRuntimeMemSize.WithLabelValues(id, dm.Source, "rss").Set(dm.MemoryRSS)
						dynoRuntimeMemSize.WithLabelValues(id, dm.Source, "swap").Set(dm.MemorySwap)
						dynoRuntimeMemSize.WithLabelValues(id, dm.Source, "total").Set(dm.MemoryTotal)

						dynoRuntimeMemPages.WithLabelValues(id, dm.Source, "in").Set(float64(dm.MemoryPgpgin))
						dynoRuntimeMemPages.WithLabelValues(id, dm.Source, "out").Set(float64(dm.MemoryPgpgout))
					}

					// Dyno log-runtime-metrics load messages
				case bytes.Contains(msg, dynoLoadMsgSentinel):
					s.maybeUpdateRecentTokens(destination.Name, id)

					dynoLoadLinesCounter.Inc()
					dm := dynoLoadMsg{}
					err := logfmt.Unmarshal(msg, &dm)
					if err != nil {
						handleLogFmtParsingError(msg, err)
						continue
					}

					if dm.Source != "" {
						destination.PostPoint(
							point{
								id,
								dynoLoad,
								[]interface{}{timestamp, dm.Source, dm.LoadAvg1Min, dm.LoadAvg5Min, dm.LoadAvg15Min, dynoType(dm.Source)},
							},
						)

						dynoRuntimeLoad.WithLabelValues(id, dm.Source, "1m").Set(dm.LoadAvg1Min)
						dynoRuntimeLoad.WithLabelValues(id, dm.Source, "5m").Set(dm.LoadAvg5Min)
						dynoRuntimeLoad.WithLabelValues(id, dm.Source, "15m").Set(dm.LoadAvg15Min)
					}

				// unknown
				default:
					unknownHerokuLinesCounter.Inc()
					if debug {
						log.Printf("Unknown Heroku Line - Header: PRI: %s, Time: %s, Hostname: %s, Name: %s, ProcId: %s, MsgId: %s - Body: %s",
							header.PrivalVersion,
							header.Time,
							header.Hostname,
							header.Name,
							header.Procid,
							header.Msgid,
							string(msg),
						)
					}
				}
			}

		// non heroku lines
		default:
			unknownUserLinesCounter.Inc()
			if debug {
				log.Printf("Unknown User Line - Header: PRI: %s, Time: %s, Hostname: %s, Name: %s, ProcId: %s, MsgId: %s - Body: %s",
					header.PrivalVersion,
					header.Time,
					header.Hostname,
					header.Name,
					header.Procid,
					header.Msgid,
					string(msg),
				)
			}
		}
	}

	linesCounter.Add(float64(linesCounterInc))

	batchSizeHistogram.Observe(float64(linesCounterInc))

	parseTimer.Observe(time.Since(parseStart).Seconds())

	w.WriteHeader(http.StatusNoContent)
}
