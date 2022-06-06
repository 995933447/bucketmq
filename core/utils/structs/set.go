package structs

type Uint32Set struct {
	uint32KeyMap map[uint32]struct{}
}

func (s *Uint32Set) Put(offset uint32) {
	if s.uint32KeyMap == nil {
		s.uint32KeyMap = make(map[uint32]struct{})
	}
	s.uint32KeyMap[offset] = struct{}{}
}

func (s *Uint32Set) Get(offset uint32) {
	if s.uint32KeyMap == nil {
		s.uint32KeyMap = make(map[uint32]struct{})
	}
	s.uint32KeyMap[offset] = struct{}{}
}

func (s *Uint32Set) Exist(offset uint32) bool {
	if s.uint32KeyMap == nil {
		return false
	}
	_, ok := s.uint32KeyMap[offset]
	return ok
}
