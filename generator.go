package rdf

import (
	"fmt"
	"sync"
)

// BNodeGenerator defines an interface for an generator that can be used to
// generate blank node IDs in incrementing order
type BNodeGenerator interface {
	// Gen should generate new blank with unique values
	Gen() Blank
}

// default generator implementation
type bNodeGenerator struct {
	counter uint   // incrementing node counter
	format  string // formatting string when outputting node IDs
	lock    sync.Locker
}

// BNodeGeneratorOptions defines options that can be given to blank node
// generators when initializing
type BNodeGeneratorOptions struct {
	// IDFormat sets the formatting string used for incrementing IDs.
	// If empty, default "_:b%06d" is used.
	// The format receives only one uint number as input.
	IDFormat string
	// BaseID sets the starting ID for the generator. Default is 0.
	BaseID uint
}

const bNodeIDFormat = "_:b%06d"
const bNodeIDBase = uint(0)

// NewBNodeGenerator intializes a new blank node generator with given options
func NewBNodeGenerator(options *BNodeGeneratorOptions) BNodeGenerator {
	g := &bNodeGenerator{
		counter: bNodeIDBase,
		format:  bNodeIDFormat,
		lock:    &sync.Mutex{},
	}

	if options != nil {
		g.counter = options.BaseID

		if options.IDFormat != "" {
			g.format = options.IDFormat
		}
	}

	return g
}

// Gen generates a new blank node with a new ID according to format
// and base given at initialization. ID is always BaseID for first node and
// one creater than the last for subsequent nodes.
func (g *bNodeGenerator) Gen() Blank {
	g.lock.Lock()
	id := g.counter
	g.counter++
	g.lock.Unlock()

	return Blank{id: fmt.Sprintf(g.format, id)}
}
