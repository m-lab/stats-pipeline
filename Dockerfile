FROM golang:1.15 as build
ENV CGO_ENABLED 0
ADD . /go/src/github.com/m-lab/stats-pipeline
WORKDIR /go/src/github.com/m-lab/stats-pipeline
RUN go get \
    -v \
    -ldflags "-X github.com/m-lab/go/prometheusx.GitShortCommit=$(git log -1 --format=%h)" \
    github.com/m-lab/stats-pipeline/cmd/stats-pipeline

# Now copy the built image into the minimal base image
FROM alpine:3.12
RUN apk add ca-certificates
COPY --from=build /go/bin/stats-pipeline /
COPY --from=build /go/src/github.com/m-lab/stats-pipeline/statistics /statistics
COPY --from=build /go/src/github.com/m-lab/stats-pipeline/annotation /annotation
WORKDIR /
ENTRYPOINT ["/stats-pipeline"]
