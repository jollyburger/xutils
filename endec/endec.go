package endec

import (
	"bytes"
	"compress/zlib"
	"encoding/base64"
)

type Endecoder interface {
	Encode([]byte) string
	Decode([]byte) ([]byte, error)
}

//base64
type Base64Endecoder struct {
}

func NewBase64Endecoder() *Base64Endecoder {
	return &Base64Endecoder{}
}

func (this *Base64Endecoder) Encode(b []byte) string {
	return base64.StdEncoding.EncodeToString(b)
}

func (this *Base64Endecoder) Decode(b []byte) ([]byte, error) {
	decoded, err := base64.StdEncoding.DecodeString(string(b))
	if err != nil {
		return nil, err
	}

	return []byte(decoded), nil
}

//zlib
type ZlibEndecoder struct {
}

func NewZlibEndecoder() *ZlibEndecoder {
	return &ZlibEndecoder{}
}

func (this *ZlibEndecoder) Encode(b []byte) string {
	var tmpB bytes.Buffer
	w := zlib.NewWriter(&tmpB)
	w.Write(b)
	w.Close()

	return string(tmpB.Bytes())
}

func (this *ZlibEndecoder) Decode(b []byte) ([]byte, error) {
	tmpB := bytes.NewReader(b)
	r, err := zlib.NewReader(tmpB)
	if err != nil {
		return nil, err
	}

	r.Close()

	buf := new(bytes.Buffer)
	buf.ReadFrom(r)

	return buf.Bytes(), err
}
