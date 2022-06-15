package storage

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	tempDir, _ = ioutil.TempDir("", "")
)

func TestMain(m *testing.M) {
	defer os.RemoveAll(tempDir)
	m.Run()
}

func TestOpenFile(t *testing.T) {

	fs, err := OpenFile(tempDir, false)
	assert.Nil(t, err)
	defer fs.Close()

	_, err = OpenFile(tempDir, false)
	assert.Error(t, err)

}

func TestFileStorage_Create(t *testing.T) {

	fmt.Println(tempDir)

	fs, err := OpenFile(tempDir, false)
	assert.Nil(t, err)

	defer fs.Close()

	fd := FileDesc{
		Type: FileTypeManifest,
		Num:  1,
	}

	writer, err := fs.Create(fd)
	assert.Nil(t, err)

	content := []byte("test fs create")
	n, err := writer.Write(content)
	assert.Nil(t, err)
	assert.EqualValues(t, len(content), n)

	err = writer.Sync()
	assert.Nil(t, err)

	writer.Close()

}

func TestFileStorage_Open(t *testing.T) {

	fmt.Println(tempDir)

	fs, err := OpenFile(tempDir, false)
	assert.Nil(t, err)
	defer os.RemoveAll(tempDir)
	defer fs.Close()

	fd := FileDesc{
		Type: FileTypeManifest,
		Num:  1,
	}

	writer, err := fs.Create(fd)
	assert.Nil(t, err)

	content := []byte("test fs create")
	n, err := writer.Write(content)
	assert.Nil(t, err)
	assert.EqualValues(t, len(content), n)

	err = writer.Sync()
	assert.Nil(t, err)

	writer.Close()

	reader, err := fs.Open(fd)
	assert.Nil(t, err)
	rContent, err := ioutil.ReadAll(reader)
	assert.Nil(t, err)
	assert.EqualValues(t, rContent, content)
	defer reader.Close()

}

func TestFileStorage_SetMeta(t *testing.T) {
	fmt.Println(tempDir)

	fs, err := OpenFile(tempDir, false)
	assert.Nil(t, err)
	//defer os.RemoveAll(tempDir)
	defer fs.Close()

	fd := FileDesc{
		Type: FileTypeManifest,
		Num:  1,
	}

	writer, _ := fs.Create(fd)
	_, _ = writer.Write([]byte("hello world"))
	writer.Sync()
	writer.Close()

	err = fs.SetMeta(fd)
	assert.Nil(t, err)

	fdG, err := fs.GetMeta()
	assert.Nil(t, err)
	assert.EqualValues(t, fdG.Num, fd.Num)
	assert.EqualValues(t, fdG.Type, fd.Type)
	fmt.Println(fdG)
}

func TestFileStorage_GetMeta(t *testing.T) {
	fmt.Println(tempDir)

	fs, err := OpenFile(tempDir, false)
	assert.Nil(t, err)
	//defer os.RemoveAll(tempDir)
	defer fs.Close()

	fd := FileDesc{
		Type: FileTypeManifest,
		Num:  1,
	}

	writer, _ := fs.Create(fd)
	_, _ = writer.Write([]byte("hello world"))
	writer.Sync()
	writer.Close()

	err = fs.SetMeta(fd)
	assert.Nil(t, err)

	fdG, err := fs.GetMeta()
	assert.Nil(t, err)
	assert.EqualValues(t, fdG.Num, fd.Num)
	assert.EqualValues(t, fdG.Type, fd.Type)
	fmt.Println(fdG)

	maxFd := FileDesc{
		Type: FileTypeManifest,
		Num:  2,
	}

	writer, err = fs.Create(maxFd)
	assert.Nil(t, err)
	defer writer.Close()

	writer, err = os.Create(filepath.Join(tempDir, "CURRENT.000002"))
	assert.Nil(t, err)
	writer.Write([]byte("MANIFEST-000002\n"))
	defer writer.Close()

	fdG, err = fs.GetMeta()
	fmt.Println(fdG)
	assert.Nil(t, err)
	assert.EqualValues(t, maxFd.Num, fdG.Num)
	assert.EqualValues(t, maxFd.Type, fdG.Type)

}
