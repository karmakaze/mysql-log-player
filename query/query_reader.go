package query

import (
	"bufio"
	"bytes"
	"io"
	"regexp"
	"strings"
)

var (
	clientRegex = regexp.MustCompile("^# User@Host: [^ ]+ @ ([^ ]+) \\[]\n$")
)

type Reader struct {
	source io.ReadCloser
	reader *bufio.Reader
	eof    bool
	buffer bytes.Buffer
}

func NewReader(source io.ReadCloser) (*Reader, error) {
	return &Reader{
			source: source,
			reader: bufio.NewReader(source),
		},
		nil
}

func (r *Reader) Read() (*Query, error) {
	if r.eof {
		return nil, io.EOF
	}

	r.buffer.Reset()
	query := &Query{}

	readPrelude := false
	for {
		line, err := r.reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				r.eof = true
				return query, nil
			}
			return nil, err
		}

		if strings.HasPrefix(line, "#") {
			if readPrelude {
				query.SQL = strings.TrimRight(r.buffer.String(), "\n")
				return query, nil
			}

			if matches := clientRegex.FindStringSubmatch(line); len(matches) == 2 {
				query.Client = matches[1]
			}
		} else {
			readPrelude = true
			r.buffer.WriteString(line)
		}
	}
}

func (r *Reader) Close() {
	r.source.Close()
}
