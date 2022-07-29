package utils

// Releaser 解引用
type Releaser interface {
	UnRef()
}

type ReleaserSetter interface {
	SetReleaser(r Releaser)
}

type BasicReleaser struct {
	released bool
	releaser Releaser
}

func (br *BasicReleaser) SetReleaser(r Releaser) {
	br.releaser = r
}

func (br *BasicReleaser) Released() bool {
	return br.released
}

func (br *BasicReleaser) UnRef() {

	if !br.released {
		if br.releaser != nil {
			br.releaser.UnRef()
		}
		br.released = true
	}
}
