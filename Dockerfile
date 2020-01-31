FROM golang:onbuild
RUN mkdir /app
ADD . /app
WORKDIR /app
COPY main.go ./
RUN go build -o go-enlyze .
ENTRYPOINT ["./go-enlyze", "-path=/", "-interval=10ns"]
