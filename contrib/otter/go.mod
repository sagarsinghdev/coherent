// The Otter backend adapter lives in its own module so the core coherent library
// stays dependency-free. Import it only if you want an Otter-backed Cache.
module github.com/sagarsinghdev/coherent/contrib/otter

go 1.24.0

require (
	github.com/maypok86/otter/v2 v2.3.0
	github.com/sagarsinghdev/coherent v0.1.0
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/stretchr/testify v1.11.1 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

// During in-repo development the core module is consumed from the tree. When
// depending on a tagged release, drop this replace and rely on the require above.
replace github.com/sagarsinghdev/coherent => ../..
