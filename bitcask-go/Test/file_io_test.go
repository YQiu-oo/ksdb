package Test

import (
	fio2 "bitcask-go/fio"
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

func destory(path string) {
	if err := os.RemoveAll(path); err != nil {
		panic(err)
	}

}

func TestNewFileIO(t *testing.T) {
	path := filepath.Join("D:", "TestFile1.data")
	fio, err := fio2.NewFileIO(path)
	defer destory(path)

	assert.Nil(t, err)

	assert.NotNil(t, fio)

}

func TestFileIO_Read(t *testing.T) {
	path := filepath.Join("D:", "TestFile.data")
	fio, err := fio2.NewFileIO(path)
	defer destory(path)

	assert.Nil(t, err)
	assert.NotNil(t, fio)
	_, err = fio.Write([]byte("key-a"))
	assert.Nil(t, err)

	b := make([]byte, 5)
	a, err := fio.Read(b, 0)
	t.Log(b, a)
	assert.Nil(t, err)
	assert.Equal(t, []byte("key-a"), b)

}
