FROM golang:1.19 as builder

WORKDIR /
COPY . .
RUN go build -o /bin/app

FROM alpine:latest
COPY --from=builder /bin/app /bin/app
ENTRYPOINT ["/bin/app"]
