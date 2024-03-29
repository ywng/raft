FROM golang:1.11.0
WORKDIR /go/src/github.com/raft/server
COPY server .
COPY pb ../pb

RUN go get -v ./...
RUN go install -v ./...

EXPOSE 3000 3001
CMD ["server"]
