package conn

import (
	"github.com/siddontang/go-mysql/client"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/500px/go-utils/chatty_logger"
)

type SiddontangOpener struct {
	hostPort string
	user     string
	pass     string
	dbName   string
}

type SiddontangConn struct {
	conn *client.Conn
}

type SiddontangRows struct {
	result *mysql.Result
}

type SiddontangRow  struct {
	result *mysql.Result
}

func NewSiddontangOpener(hostPort string, user, pass string, dbName string) *SiddontangOpener {
	return &SiddontangOpener{
		hostPort: hostPort,
		user:     user,
		pass:     pass,
		dbName:   dbName,
	}
}

func (m *SiddontangOpener) Open() (DBConn, error) {
	conn, err := client.Connect(m.hostPort, m.user, m.pass, m.dbName)
	logger.Debugf("Error connecting with (%v, %v, %v, %v)", m.hostPort, m.user, m.pass, m.dbName)
	if err != nil {
		return nil, err
	}
	return SiddontangConn{conn}, nil
}

func (c SiddontangConn) Query(query string, args ...interface{}) (Rows, error) {
	result, err := c.conn.Execute(query, args...)
	if err != nil {
		return nil, err
	}
	return &SiddontangRows{result}, nil
}

func (rs SiddontangRows) Close() error {
	return nil
}

func (rs SiddontangRows) First() (Row, error) {
	return &SiddontangRow{rs.result}, nil
}

func (r SiddontangRow) Int(name string) (int32, error) {
	logger.Infof("SiddontangRow.Int(%s): status = %v", name, r.result.Status)
	valInt64, err := r.result.GetIntByName(0, name)
	if err != nil {
		return 0, err
	}
	return int32(valInt64), nil
}
