package structs

type Uint64Set struct {
	uint64KeyMap map[uint64]struct{}
}

func (s *Uint64Set) Put(offset uint64) {
	if s.uint64KeyMap == nil {
		s.uint64KeyMap = make(map[uint64]struct{})
	}
	s.uint64KeyMap[offset] = struct{}{}
}

func (s *Uint64Set) Exist(offset uint64) bool {
	if s.uint64KeyMap == nil {
		return false
	}
	_, ok := s.uint64KeyMap[offset]
	return ok
}
