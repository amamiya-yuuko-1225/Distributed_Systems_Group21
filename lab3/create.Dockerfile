FROM golang:1.19

ENV ADDRESS=127.0.0.1
ENV SSH_PORT=9082
ENV TIME_STABILIZE=25000
ENV TIME_FIX_FINGERS=10000
ENV TIME_CHECK_PREDECESSOR=30000
ENV TIME_BACKUP=60000
ENV SUCCESSOR_LIST_SIZE=4
ENV PORT=8082
EXPOSE ${PORT}
EXPOSE ${SSH_PORT}

RUN apt update -y

WORKDIR /usr/src/app

COPY go.mod go.sum  ./
RUN go mod download  && go mod verify

COPY . .
RUN go build -o /usr/local/bin/app .
ENTRYPOINT app -a "${ADDRESS}" -p "${PORT}" -sp "${SSH_PORT}" -ts "${TIME_STABILIZE}" -tff "${TIME_FIX_FINGERS}" -tcp "${TIME_CHECK_PREDECESSOR}" -tb "${TIME_BACKUP}" -r "${SUCCESSOR_LIST_SIZE}"