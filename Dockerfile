FROM golang:1.11-alpine

# install build dependencies
RUN apk --no-cache add git curl build-base bash

# install glide
RUN curl https://glide.sh/get | sh

# get sqlbeat
#  WORKAROUND! github.com/adibendahan/sqlbeat triggers a error with go get
#   RUN go get github.com/adibendahan/sqlbeat
RUN git clone https://github.com/adibendahan/sqlbeat.git /go/src/github.com/adibendahan/sqlbeat
RUN go get golang.org/x/crypto/md4
#  WORKAROUND END :)  Signed by llonchj

WORKDIR /go/src/github.com/adibendahan/sqlbeat

RUN glide update --no-recursive
RUN make 

FROM alpine:latest
RUN apk --no-cache add ca-certificates tzdata

COPY --from=0 /go/src/github.com/adibendahan/sqlbeat/sqlbeat /usr/local/bin/

ADD sqlbeat.yml /etc/sqlbeat.yml

CMD ["/usr/local/bin/sqlbeat", "-e", "-c", "/etc/sqlbeat.yml"]
