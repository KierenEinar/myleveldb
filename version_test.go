package myleveldb

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestVersionStaging(t *testing.T) {
	vs := &VersionStaging{
		base: &Version{
			id:     1,
			levels: nil,
			session: &Session{
				ntVersionId: 0,
				icmp: func(a, b interface{}) int {
					return bytes.Compare(a.(internalKey), b.(internalKey))
				},
			},
		},
		scratch: nil,
	}

	p := &SessionRecord{
		atRecords: []atRecord{
			{
				level: 0,
				num:   0,
				size:  100,
				min:   []byte("aaaaaa"),
				max:   []byte("bbbbbb"),
			},
			{
				level: 0,
				num:   1,
				size:  100,
				min:   []byte("cccccc"),
				max:   []byte("dddddd"),
			},
			{
				level: 0,
				num:   2,
				size:  100,
				min:   []byte("eeeeee"),
				max:   []byte("ffffff"),
			},
			{
				level: 0,
				num:   3,
				size:  100,
				min:   []byte("aaaaaa"),
				max:   []byte("ffffff"),
			},
			{
				level: 1,
				num:   4,
				size:  100,
				min:   []byte("iiiiii"),
				max:   []byte("jjjjjj"),
			},
		},
	}

	vs.commit(p)

	assert.EqualValues(t, len(vs.scratch), 2)
	assert.EqualValues(t, len(vs.scratch[0].deleted), 0)
	assert.EqualValues(t, len(vs.scratch[1].deleted), 0)
	assert.EqualValues(t, vs.scratch[0].added[0].num, 0)
	assert.EqualValues(t, vs.scratch[0].added[1].num, 1)
	assert.EqualValues(t, vs.scratch[0].added[2].num, 2)
	assert.EqualValues(t, vs.scratch[0].added[3].num, 3)
	assert.EqualValues(t, vs.scratch[1].added[4].num, 4)

	nv := vs.finish()
	assert.EqualValues(t, len(nv.levels), 2)
	assert.EqualValues(t, nv.levels[0][0].fd.Num, 0)
	assert.EqualValues(t, nv.levels[0][1].fd.Num, 1)
	assert.EqualValues(t, nv.levels[0][2].fd.Num, 2)
	assert.EqualValues(t, nv.levels[0][3].fd.Num, 3)
	assert.EqualValues(t, nv.levels[1][0].fd.Num, 4)

	vs = &VersionStaging{
		base: nv,
	}

	p = &SessionRecord{
		atRecords: []atRecord{
			{
				level: 1,
				num:   5,
				size:  100,
				min:   []byte("kkkkkk"),
				max:   []byte("llllll"),
			},
			{
				level: 1,
				num:   6,
				size:  100,
				min:   []byte("mmmmmm"),
				max:   []byte("nnnnnn"),
			},
		},
		dlRecords: []dlRecord{
			{
				level: 1,
				num:   5,
			},
		},
	}

	vs.commit(p)

	assert.EqualValues(t, len(vs.scratch), 2)
	assert.EqualValues(t, len(vs.scratch[0].added), 0)
	assert.EqualValues(t, len(vs.scratch[0].deleted), 0)
	assert.EqualValues(t, len(vs.scratch[1].added), 1)
	assert.EqualValues(t, len(vs.scratch[1].deleted), 0)

	assert.EqualValues(t, vs.scratch[0].added[0].num, 0)
	assert.EqualValues(t, vs.scratch[1].added[6].num, 6)

	nv = vs.finish()

	assert.EqualValues(t, len(nv.levels), 2)
	assert.EqualValues(t, nv.levels[0][0].fd.Num, 0)
	assert.EqualValues(t, nv.levels[0][1].fd.Num, 1)
	assert.EqualValues(t, nv.levels[0][2].fd.Num, 2)
	assert.EqualValues(t, nv.levels[0][3].fd.Num, 3)
	assert.EqualValues(t, nv.levels[1][0].fd.Num, 4)
	assert.EqualValues(t, nv.levels[1][1].fd.Num, 6)

	vs = &VersionStaging{
		base: nv,
	}

	p = &SessionRecord{
		dlRecords: []dlRecord{
			{
				level: 1,
				num:   6,
			},
		},
	}

	vs.commit(p)

	assert.EqualValues(t, len(vs.scratch), 2)
	assert.EqualValues(t, len(vs.scratch[0].added), 0)
	assert.EqualValues(t, len(vs.scratch[0].deleted), 0)
	assert.EqualValues(t, len(vs.scratch[1].added), 0)
	assert.EqualValues(t, len(vs.scratch[1].deleted), 1)

	nv = vs.finish()

	assert.EqualValues(t, len(nv.levels), 2)
	assert.EqualValues(t, nv.levels[0][0].fd.Num, 0)
	assert.EqualValues(t, nv.levels[0][1].fd.Num, 1)
	assert.EqualValues(t, nv.levels[0][2].fd.Num, 2)
	assert.EqualValues(t, nv.levels[0][3].fd.Num, 3)
	assert.EqualValues(t, nv.levels[1][0].fd.Num, 4)

	vs = &VersionStaging{
		base: nv,
	}

	p = &SessionRecord{
		dlRecords: []dlRecord{
			{
				level: 1,
				num:   4,
			},
		},
	}

	vs.commit(p)

	assert.EqualValues(t, len(vs.scratch), 2)
	assert.EqualValues(t, len(vs.scratch[0].added), 0)
	assert.EqualValues(t, len(vs.scratch[0].deleted), 0)
	assert.EqualValues(t, len(vs.scratch[1].added), 0)
	assert.EqualValues(t, len(vs.scratch[1].deleted), 1)

	nv = vs.finish()

	assert.EqualValues(t, len(nv.levels), 1)
	assert.EqualValues(t, nv.levels[0][0].fd.Num, 0)
	assert.EqualValues(t, nv.levels[0][1].fd.Num, 1)
	assert.EqualValues(t, nv.levels[0][2].fd.Num, 2)
	assert.EqualValues(t, nv.levels[0][3].fd.Num, 3)

}
