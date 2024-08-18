package Test

import (
	"bitcask-go/data"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestDataOpen(t *testing.T) {
	file, err := data.OpenDataFile(os.TempDir(), 0)

	assert.Nil(t, err)
	assert.NotNil(t, file)

	t.Log(os.TempDir())
}
