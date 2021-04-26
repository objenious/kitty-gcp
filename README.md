# kitty-gcp

[![Travis-CI](https://travis-ci.org/objenious/kitty-gcp.svg?branch=master)](https://travis-ci.org/objenious/kitty-gcp)  [![GoDoc](https://godoc.org/github.com/objenious/kitty-gcp?status.svg)](http://godoc.org/github.com/objenious/kitty-gcp)
[![GoReportCard](https://goreportcard.com/badge/github.com/objenious/kitty-gcp)](https://goreportcard.com/report/github.com/objenious/kitty-gcp)

`go get github.com/objenious/kitty-gcp`

## Status: alpha - breaking changes might happen

kitty-gcp adds support for Google Cloud Platform to [kitty](https://github.com/objenious/kitty).

## Pub/Sub

Connect to Pub/Sub :
```
tr := pubsub.NewTransport(ctx, "project-id").
  Endpoint(subscriptionName, endpoint, Decoder(decodeFunc))
err := kitty.NewServer(tr).Run(ctx)
```

If your service needs to run on Kubernetes, you also need to have a http transport for keep-alives :
```
tr := pubsub.NewTransport(ctx, "project-id").
  Endpoint(subscriptionName, endpoint, Decoder(decodeFunc))
h := kitty.NewHTTPTransport(kitty.Config{HTTPPort: 8080})
err := kitty.NewServer(tr, h).Run(ctx)
```
