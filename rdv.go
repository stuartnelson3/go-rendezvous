package rendezvous

type Rendezvous struct {
	nodes map[string]int
	nstr  []string
	nhash []uint64
	hash  Hasher
}

type Hasher func(s string) uint64

func New(nodes []string, hash Hasher) *Rendezvous {
	r := &Rendezvous{
		nodes: make(map[string]int, len(nodes)),
		nstr:  make([]string, len(nodes)),
		nhash: make([]uint64, len(nodes)),
		hash:  hash,
	}

	for i, n := range nodes {
		r.nodes[n] = i
		r.nstr[i] = n
		r.nhash[i] = hash(n)
	}

	return r
}

func (r *Rendezvous) Lookup(k string) string {
	return r.nstr[r.midx(k)]
}

func (r *Rendezvous) LookupN(k string, n uint) []string {
	midx := r.midx(k)
	c := int(n)
	idxs := make([]int, c)
	nl := len(r.nodes)
	step := nl / c
	for i := 0; i < c; i++ {
		if idx := midx + (i * step); idx < nl {
			idxs[i] = idx
		} else {
			idxs[i] = idx % nl
		}
	}

	found := make([]string, 0, c)
	for _, idx := range idxs {
		found = append(found, r.nstr[idx])
	}
	return found
}

func (r *Rendezvous) Add(node string) {
	r.nodes[node] = len(r.nstr)
	r.nstr = append(r.nstr, node)
	r.nhash = append(r.nhash, r.hash(node))
}

func (r *Rendezvous) Remove(node string) {
	// find index of node to remove
	nidx := r.nodes[node]

	// remove from the slices
	r.nstr = append(r.nstr[:nidx], r.nstr[nidx+1:]...)
	r.nhash = append(r.nhash[:nidx], r.nhash[nidx+1:]...)

	// update the map
	delete(r.nodes, node)
	if nidx < len(r.nstr) {
		moved := r.nstr[nidx]
		r.nodes[moved] = nidx
	}
}

func xorshiftMult64(x uint64) uint64 {
	x ^= x >> 12 // a
	x ^= x << 25 // b
	x ^= x >> 27 // c
	return x * 2685821657736338717
}

func (r *Rendezvous) midx(k string) int {
	khash := r.hash(k)

	var midx int
	var mhash = xorshiftMult64(khash ^ r.nhash[0])

	for i, nhash := range r.nhash[1:] {
		if h := xorshiftMult64(khash ^ nhash); h > mhash {
			midx = i + 1
			mhash = h
		}
	}

	return midx
}
