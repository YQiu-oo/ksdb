package fio

const DataFilePerm = 0644

type FileIOType = byte

const (
	StandardFIO FileIOType = iota
	MemoryMap
)

type IOManager interface {
	Read([]byte, int64) (int, error)
	Write([]byte) (int, error)
	Sync() error
	Close() error
	Size() (int64, error)
}

func NewIOManager(filename string, typ FileIOType) (IOManager, error) {
	switch typ {
	case StandardFIO:
		return NewFileIO(filename)
	case MemoryMap:
		return NewMMapIOManager(filename)
	default:
		panic("Unsupported IO type")
	}
}
