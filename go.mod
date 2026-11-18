module sipbridge

go 1.24.2

require (
	github.com/coder/websocket v1.8.15
	github.com/emiago/sipgo v0.0.0
	github.com/google/uuid v1.6.0
	github.com/redis/go-redis/v9 v9.17.3
	golang.org/x/sync v0.16.0
	google.golang.org/protobuf v1.36.12
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/gobwas/httphead v0.1.0 // indirect
	github.com/gobwas/pool v0.2.1 // indirect
	github.com/gobwas/ws v1.3.2 // indirect
	github.com/icholy/digest v1.2.0 // indirect
	golang.org/x/sys v0.24.0 // indirect
)

replace github.com/emiago/sipgo => ./third_party/sipgo
