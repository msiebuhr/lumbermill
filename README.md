[![Travis](https://img.shields.io/travis/heroku/lumbermill.svg)](https://travis-ci.org/heroku/lumbermill)
[![GoDoc](https://godoc.org/github.com/heroku/lumbermill?status.svg)](http://godoc.org/github.com/heroku/lumbermill)

# Lumbermill

This is a Go app which takes Heroku Log drains and parses the router and dyno information, and then pushes metrics to influxdb.

## Setup
### Setup Prometheus

Have it fetch metrics on `/metrics`, and it'll start downloading.

### Deploy to Heroku

[![Deploy to Heroku](https://www.herokucdn.com/deploy/button.png)](https://heroku.com/deploy)

### Add the drain to an app

```
heroku drains:add https://<lumbermill_app>.herokuapp.com/drain --app <the-app-to-mill-for>
```

You'll then start getting metrics in your prometheus host!

### Environment Variables

* `CRED_STORE`: `user1:pass1|user2:pass2|userN:passN` -- Basic Auth credentials for HTTP endpoints.
* `DEBUG`: Turn on debug mode
* `PORT`: 
