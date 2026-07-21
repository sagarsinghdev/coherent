// The gRPC streaming transport for coherent lives in its own module so the core
// library stays dependency-free. Import it only if you use the gRPC source.
module github.com/sagarsinghdev/coherent/examples/grpc

go 1.24

require (
	github.com/sagarsinghdev/coherent v0.1.0
	google.golang.org/grpc v1.71.0
	google.golang.org/protobuf v1.36.6
)

require (
	golang.org/x/net v0.34.0 // indirect
	golang.org/x/sys v0.29.0 // indirect
	golang.org/x/text v0.21.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250115164207-1a7da9e5054f // indirect
)

// During in-repo development the core module is consumed from the tree. When
// depending on a tagged release, drop this replace and rely on the require above.
replace github.com/sagarsinghdev/coherent => ../..
