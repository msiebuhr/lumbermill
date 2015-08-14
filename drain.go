package main

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/bmizerany/lpx"
	"github.com/kr/logfmt"

	"github.com/msiebuhr/routefinder"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	// TokenPrefix contains the prefix for non-heroku tokens.
	TokenPrefix = []byte("t.")
	// Heroku contains the prefix for heroku tokens.
	Heroku = []byte("heroku")

	// go-metrics Instruments
	lumbermillErrorCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace:   "lumbermill",
		Name:        "errors",
		Help:        "Lumbermill parsing errors",
	}, []string{"error"})

	batchCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "lumbermill",
		Name:      "batch",
		Help:      "x",
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

	dynoStatusLines = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "lumbermill",
		Name:      "dyno_lines",
		Help:      "Number of counted dyno status lines",
	}, []string{"type"})

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
	httpRequestDurationMicroseconds = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Name: "http_request_duration_microseconds",
		Help: "The HTTP request latencies in microseconds.",
	}, []string{"job", "instance", "handler", "status"})
	httpRequestConnectMicroseconds = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Name: "http_request_connect_microseconds",
		Help: "The HTTP connect latencies in microseconds.",
	}, []string{"job", "instance"})
	httpRequestsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "http_requests_total",
		Help: "Total number of HTTP requests made.",
	}, []string{"job", "instance", "handler", "code"})
	httpResponseSizeBytes = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Name: "http_response_size_bytes",
		Help: "The HTTP response sizes in bytes.",
	}, []string{"job", "instance", "handler", "status"})
	routerServiceError = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "heroku_router_error_count",
		Help: "Number of router errors",
	}, []string{
		"job",
		"instance",
		"route",
		"hcode",
	})
	dynoServiceError = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "heroku_dyno_error_count",
		Help: "Number of dyno errors",
	}, []string{
		"job",
		"instance",
		"rcode",
	})
	dynoRuntimeMemSize = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "heroku_runtime_memory_mb",
		Help: "Heroku memory use",
	}, []string{
		"job",
		"instance",
		"type",
	})
	dynoRuntimeMemPages = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "heroku_runtime_memory_pages",
		Help: "Heroku memory use",
	}, []string{
		"job",
		"instance",
		"dir",
	})
	dynoRuntimeLoad = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "heroku_runtime_load",
		Help: "Heroku memory use",
	}, []string{
		"job",
		"instance",
		"span",
	})
	loadAvg1 = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "node_load1",
		Help: "1m load avgerage.",
	}, []string{"job", "instance"})

	// Route lookups
	routesLock sync.Mutex
	routes     map[string]*routefinder.Routefinder
)

func init() {
	prometheus.MustRegister(lumbermillErrorCounter)

	prometheus.MustRegister(batchCounter)

	prometheus.MustRegister(linesCounter)
	prometheus.MustRegister(routerErrorLinesCounter)
	prometheus.MustRegister(routerLinesCounter)

	prometheus.MustRegister(dynoStatusLines)

	prometheus.MustRegister(dynoErrorLinesCounter)
	prometheus.MustRegister(dynoMemLinesCounter)
	prometheus.MustRegister(dynoLoadLinesCounter)
	prometheus.MustRegister(unknownHerokuLinesCounter)
	prometheus.MustRegister(unknownUserLinesCounter)
	prometheus.MustRegister(parseTimer)
	prometheus.MustRegister(batchSizeHistogram)

	// Actual data-capture
	prometheus.MustRegister(httpRequestDurationMicroseconds)
	prometheus.MustRegister(httpRequestConnectMicroseconds)
	prometheus.MustRegister(httpRequestsTotal)
	prometheus.MustRegister(httpResponseSizeBytes)
	prometheus.MustRegister(routerServiceError)
	prometheus.MustRegister(dynoRuntimeMemSize)
	prometheus.MustRegister(dynoRuntimeMemPages)
	prometheus.MustRegister(dynoRuntimeLoad)
	prometheus.MustRegister(loadAvg1)

	routes = make(map[string]*routefinder.Routefinder)
}

func getRoutes(appName string) *routefinder.Routefinder {
	plainName := strings.ToLower(appName)
	routesLock.Lock()
	defer routesLock.Unlock()

	// Is it already there?
	if finder, ok := routes[plainName]; ok {
		return finder
	}

	// Add it
	routes[plainName] = &routefinder.Routefinder{}

	// Is it in the env?
	names := []string{
		"routes_" + appName,                    // routes_appName
		"routes_" + plainName,                  // routes_appName
		"ROUTES_" + strings.ToUpper(plainName), // routes_appName
	}

	for _, name := range names {
		env := os.Getenv(name)
		if env != "" {
			routes[plainName].Set(env)
		}
	}

	return routes[plainName]
}

// Dyno's are generally reported as "<type>.<#>"
// Extract the <type> and return it
func dynoType(what string) string {
	s := strings.Split(what, ".")
	return s[0]
}

func handleLogFmtParsingError(msg []byte, err error) {
	lumbermillErrorCounter.WithLabelValues("logfmt_parse").Inc()
	log.Printf("logfmt unmarshal error(%q): %q\n", string(msg), err)
}

// "Parse tree" from hell
func (s *server) serveDrain(w http.ResponseWriter, r *http.Request) {
	s.Add(1)
	defer s.Done()

	w.Header().Set("Content-Length", "0")

	if r.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		lumbermillErrorCounter.WithLabelValues("drain_wrong_method").Inc();
		return
	}

	id := r.Header.Get("Logplex-Drain-Token")

	batchCounter.Inc()

	parseStart := time.Now()
	lp := lpx.NewReader(bufio.NewReader(r.Body))

	linesCounterInc := 0

	// TODO: It would be nice to convert query parameters to prometheus key/value pairs
	q := r.URL.Query()
	if q.Get("app") == "" {
		// Metric for invalid batches!
		w.WriteHeader(http.StatusMethodNotAllowed)
		lumbermillErrorCounter.WithLabelValues("no_appname_query").Inc()
		return
	}
	app := q.Get("app")

	// Get any routes for this app
	routes := getRoutes(app)

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
			lumbermillErrorCounter.WithLabelValues("token_missing").Inc()
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
					lumbermillErrorCounter.WithLabelValues("parsing").Inc()
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

				// Try decoding the path to a known route
				handler, _ := routes.Lookup(rm.Path)

				if handler != "" {
					handler = rm.Method + " " + handler
				} else {
					handler = rm.Method + " ?"
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

					routerServiceError.WithLabelValues(app, rm.Dyno, handler, fmt.Sprint(re.Code)).Inc()

				// If the app is blank (not pushed) we don't care
				// do nothing atm, increment a counter
				case bytes.Contains(msg, keyCodeBlank), bytes.Contains(msg, keyDescBlank):
					routerBlankLinesCounter.Inc()

				// likely a standard router log
				default:
					routerLinesCounter.Inc()
					destination.PostPoint(point{id, routerRequest, []interface{}{timestamp, rm.Status, rm.Service}})

					httpRequestDurationMicroseconds.WithLabelValues(app, rm.Dyno, handler, fmt.Sprint(rm.Status)).Observe(float64(rm.Service * 1000))
					httpRequestsTotal.WithLabelValues(app, rm.Dyno, handler, fmt.Sprint(rm.Status)).Inc()
					httpResponseSizeBytes.WithLabelValues(app, rm.Dyno, handler, fmt.Sprint(rm.Status)).Observe(float64(rm.Bytes))

					// This is measured before any of our code is hit, so no point in adding different code-paths as dimensions
					httpRequestConnectMicroseconds.WithLabelValues(app, rm.Dyno).Observe(float64(rm.Connect * 1000))
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
					dynoServiceError.WithLabelValues(app, string(lp.Header().Procid), fmt.Sprint(de.Code)).Inc()

				// Dyno log-runtime-metrics memory messages
				case bytes.Contains(msg, dynoMemMsgSentinel):
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

						dynoRuntimeMemSize.WithLabelValues(app, dm.Source, "cache").Set(dm.MemoryCache)
						dynoRuntimeMemSize.WithLabelValues(app, dm.Source, "rss").Set(dm.MemoryRSS)
						dynoRuntimeMemSize.WithLabelValues(app, dm.Source, "swap").Set(dm.MemorySwap)

						dynoRuntimeMemPages.WithLabelValues(app, dm.Source, "in").Set(float64(dm.MemoryPgpgin))
						dynoRuntimeMemPages.WithLabelValues(app, dm.Source, "out").Set(float64(dm.MemoryPgpgout))
					}

				// Dyno log-runtime-metrics load messages
				case bytes.Contains(msg, dynoLoadMsgSentinel):
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

						dynoRuntimeLoad.WithLabelValues(app, dm.Source, "1m").Set(dm.LoadAvg1Min)
						loadAvg1.WithLabelValues(app, dm.Source).Set(dm.LoadAvg1Min)
						dynoRuntimeLoad.WithLabelValues(app, dm.Source, "5m").Set(dm.LoadAvg5Min)
						dynoRuntimeLoad.WithLabelValues(app, dm.Source, "15m").Set(dm.LoadAvg15Min)
					}

				// Catch schedulder shutdown
				case bytes.Contains(msg, schedJobComplete):
					dynoStatusLines.WithLabelValues("scheduler_shutdown").Inc()
					schedName := string(header.Procid)

					// Remove load things
					dynoRuntimeLoad.DeleteLabelValues(app, schedName, "1m")
					loadAvg1.DeleteLabelValues(app, schedName)
					dynoRuntimeLoad.DeleteLabelValues(app, schedName, "5m")
					dynoRuntimeLoad.DeleteLabelValues(app, schedName, "15m")

					dynoRuntimeMemSize.DeleteLabelValues(app, schedName, "cache")
					dynoRuntimeMemSize.DeleteLabelValues(app, schedName, "rss")
					dynoRuntimeMemSize.DeleteLabelValues(app, schedName, "swap")

					dynoRuntimeMemPages.DeleteLabelValues(app, schedName, "in")
					dynoRuntimeMemPages.DeleteLabelValues(app, schedName, "out")

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
