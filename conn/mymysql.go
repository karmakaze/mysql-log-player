package conn

import (
	"github.com/ziutek/mymysql/mysql"
	_ "github.com/ziutek/mymysql/native"
)

type MyMySQLOpener struct {
	hostPort string
	user     string
	pass     string
	dbName   string
}

type MyMySQLConn struct {
	conn mysql.Conn
}

type MyMySQLRows struct {
	rows mysql.Result
}

type MyMySQLRow  struct {
	rows mysql.Result
	row mysql.Row
}

func NewMyMySQLOpener(hostPort string, user, pass string, dbName string) *MyMySQLOpener {
	return &MyMySQLOpener{
		hostPort: hostPort,
		user:     user,
		pass:     pass,
		dbName:   dbName,
	}
}

func (m *MyMySQLOpener) Open() (DBConn, error) {
	conn := mysql.New("tcp", "", m.hostPort, m.user, m.pass, m.dbName)
	err := conn.Connect()
	return &MyMySQLConn{conn}, err
}

func (c MyMySQLConn) Query(query string, args ...interface{}) (Rows, error) {
	_, result, err := c.conn.Query(query, args...)
	if err != nil {
		return nil, err
	}
	return &MyMySQLRows{result}, nil
}

func (rs MyMySQLRows) Close() error {
	return nil
}

func (rs MyMySQLRows) First() (Row, error) {
	row, err := rs.rows.GetFirstRow()
	if err != nil {
		return nil, err
	}
	return &MyMySQLRow{rs.rows, row}, nil
}

func (r MyMySQLRow) Int(name string) (int32, error) {
	for i, field := range r.rows.Fields() {
		if field.Name == name {
			intValue := r.row.Int(i)
			return int32(intValue), nil
		}
	}
	return 0, NoColumnError
}
