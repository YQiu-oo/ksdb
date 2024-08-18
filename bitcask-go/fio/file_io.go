package fio

import "os"

type FileIO struct {
	fd *os.File
}

func NewFileIO(path string) (*FileIO, error) {
	fd, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, DataFilePerm)
	if err != nil {
		return nil, err
	}
	return &FileIO{fd: fd}, nil

}
func (f *FileIO) Read(content []byte, offset int64) (int, error) {
	return f.fd.ReadAt(content, offset)
}
func (f *FileIO) Write(content []byte) (int, error) {
	return f.fd.Write(content)

}
func (f *FileIO) Sync() error {
	return f.fd.Sync()
}
func (f *FileIO) Close() error {
	return f.fd.Close()
}
func (f *FileIO) Size() (int64, error) {
	Stat, err := f.fd.Stat()
	if err != nil {
		return 0, err
	}
	return Stat.Size(), nil
}
