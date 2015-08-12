[![GoDoc](https://godoc.org/github.com/msiebuhr/lumbermill-prometheus?status.svg)](http://godoc.org/github.com/msiebuhr/lumbermill-prometheus)

# Lumbermill-prometheus

This is a Go app which takes Heroku Log drains and parses the router and dyno information, and then serves metrics to Prometheus.

## Setup
### Setup Prometheus

Have your prometheus-server fetch from
`https://user:password@<lumbermill_app>/metrics`. Be sure to add the option
`honor_labels: true` in the config-section, otherwise prometheus will attribute
all data coming through lumbermill as coming *from* lumbermill.

Set the username/password in `CRED_STORE`.

### Deploy to Heroku

[![Deploy to Heroku](https://www.herokucdn.com/deploy/button.png)](https://heroku.com/deploy)

### Add the drain to an app

```
heroku labs:enable log-runtime-metrics --app <the-app-to-mill-for>
heroku drains:add https://user:password@<lumbermill_app>.herokuapp.com/drain?app=<the-app-to-mill-for> --app <the-app-to-mill-for>
```

And it'll start dumping metrics to the endpoint above.  Again, set the
username/password in `CRED_STORE`.

### Environment Variables

* `CRED_STORE`: `user1:pass1|user2:pass2|userN:passN` -- Basic Auth credentials for HTTP endpoints.
* `DEBUG`: Turn on debug mode
* `PORT`: 
