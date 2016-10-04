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

type Format int

const (
	FORMAT_MYSQL_SNIFFER Format = iota
	FORMAT_VC_MYSQL_SNIFFER
)

var (
	STARTS_WITH_IPV4_COLON_PORT_COLON = regexp.MustCompile(`^([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+:[0-9]+):`)
)

type Reader struct {
	format   Format
	source   io.ReadCloser
	nextLine string
	reader   *bufio.Reader
	eof      bool
	buffer   bytes.Buffer
}

func NewReader(format Format, source io.ReadCloser) (*Reader, error) {
	return &Reader{
			format: format,
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

	inMidQuery := false
	for {
		var line string
		if r.nextLine != "" {
			line = r.nextLine
			r.nextLine = ""
		} else {
			var err error
			line, err = r.reader.ReadString('\n')
			if err != nil {
				if err == io.EOF {
					r.eof = true
					return query, nil
				}
				return nil, err
			}
		}

		if r.format == FORMAT_MYSQL_SNIFFER {
			matches := STARTS_WITH_IPV4_COLON_PORT_COLON.FindStringSubmatch(line)
			if len(matches) > 0 {
				if inMidQuery {
					r.nextLine = line
					query.SQL = strings.TrimRight(r.buffer.String(), "\n")
					return query, nil
				}

				query.Client = matches[1]
				r.buffer.WriteString(line[len(matches[0]):])
			} else {
				inMidQuery = true
				r.buffer.WriteString(line)
			}
		} else if r.format == FORMAT_VC_MYSQL_SNIFFER {
			if strings.HasPrefix(line, "#") {
				if inMidQuery {
					query.SQL = strings.TrimRight(r.buffer.String(), "\n")
					return query, nil
				}

				if matches := clientRegex.FindStringSubmatch(line); len(matches) == 2 {
					query.Client = matches[1]
				}
			} else {
				inMidQuery = true
				r.buffer.WriteString(line)
			}
		}
	}
}

func (r *Reader) Close() {
	r.source.Close()
}
