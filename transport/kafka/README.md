# Apache Kafka transport

Kafka transport plugin provides ability to make RPC calls via Apache Kafka.

**Note:** Apache Kafka v1.0.0 (or higher) is required with [headers](https://cwiki.apache.org/confluence/display/KAFKA/KIP-82+-+Add+Record+Headers) support.

## Usage
- import packages
  ```go
  import(
    "github.com/micro/go-micro"
    "github.com/micro/go-micro/client"
    "github.com/micro/go-micro/registry/mock"
    "github.com/micro/go-micro/server"
    "github.com/micro/go-micro/server/rpc"
    "github.com/micro/go-plugins/selector/named"
    "github.com/micro/go-plugins/transport/kafka"
  )
  ```
- init dependencies
  ```go
  // use named selector since load ballancing is on kafka
  selector := named.NewSelector()
  // provide mock registry to override default Consul registry
  registry := mock.NewRegistry()
  // kafka transport (default port is :9092 or provide custom settings using options)
  transport := kafka.NewTransport()
  ```
- server
  ```go
  rpcServer := rpc.NewServer(
    server.Transport(transport),
    server.Address("greeter"),
  )

  service := micro.NewService(
    micro.Server(rpcServer),
    micro.Registry(registry),
    micro.Transport(transport),
  )

  hello.RegisterGreeterHandler(service.Server(), new(Greeter))
  log.Fatal(service.Run())
  ```
- client
  ```go
  rpcClient := client.NewClient(
    client.Selector(selector),
    client.Transport(transport),
    client.Registry(registry),
  )

  greeter := hello.NewGreeterService("greeter", rpcClient)
  rsp, err := greeter.Hello(context.TODO(), &hello.HelloRequest{Name: "John"})
  if err != nil {
    fmt.Println(err)
  } else {
    fmt.Println(rsp.Greeting)
  }
  ```
