package main

import (
	"bufio"
	"bytes"
	"io"
	"strings"
)

type QueryReader struct {
	source io.ReadCloser
	reader *bufio.Reader
	eof    bool
	buffer bytes.Buffer
}

func NewQueryReader(source io.ReadCloser) (*QueryReader, error) {
	return &QueryReader{
			source: source,
			reader: bufio.NewReader(source),
		},
		nil
}

func (r *QueryReader) Read() (string, error) {
	if r.eof {
		return "", io.EOF
	}

	r.buffer.Reset()

	readPrelude := false
	for {
		line, err := r.reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				return line, nil
			}
			return "", err
		}

		if strings.HasPrefix(line, "#") {
			if readPrelude {
				return r.buffer.String(), nil
			}
		} else {
			readPrelude = true
			r.buffer.WriteString(line)
		}
	}
}

func (r *QueryReader) Close() {
	r.source.Close()
}
