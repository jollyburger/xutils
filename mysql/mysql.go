package mysql

import (
	"fmt"

	"github.com/jollyburger/xmysql"
)

type MysqlConfig struct {
	Server   string `json:"server"`
	DbName   string `json:"db"`
	UserName string `json:"username"`
	PassWd   string `json:"password"`
	Service  string `json:"service"`
}

var (
	mysqlConfig MysqlConfig
)

func InitMysql(conf MysqlConfig) {
	mysqlConfig = conf

	if mysqlConfig.Server == "" {
		panic("mysql server empty")
	}

	if mysqlConfig.DbName == "" {
		panic("mysql db name empty")
	}

	if mysqlConfig.UserName == "" {
		panic("mysql user name empty")
	}

	if mysqlConfig.PassWd == "" {
		panic("mysql password empty")
	}

	if mysqlConfig.Service == "" {
		panic("mysql service empty")
	}

	connStr := fmt.Sprintf("%s:%s@tcp(%s)/%s", mysqlConfig.UserName, mysqlConfig.PassWd, mysqlConfig.Server, mysqlConfig.DbName)
	err := xmysql.RegisterMysqlService(mysqlConfig.Service, connStr, "")
	if err != nil {
		panic(fmt.Sprintf("initialize db instance error: %v", err))
	}
}

func GetMysqlConfig() MysqlConfig {
	return mysqlConfig
}

type MysqlDbClient struct {
	service string
	dbName  string
}

func GetSharedMysqlDbClient() *MysqlDbClient {
	c := new(MysqlDbClient)

	c.service = mysqlConfig.Service
	c.dbName = mysqlConfig.DbName

	return c
}

func (this *MysqlDbClient) Insert(sql string, args ...interface{}) (lastInsertId int64, err error) {
	return xmysql.Insert(this.service, sql, args...)
}

func (this *MysqlDbClient) Update(sql string, args ...interface{}) (rowsAffected int64, err error) {
	return xmysql.Update(this.service, sql, args...)
}

func (this *MysqlDbClient) Delete(sql string, args ...interface{}) (rowsAffected int64, err error) {
	return xmysql.Delete(this.service, sql, args...)
}

func (this *MysqlDbClient) Select(sql string, args ...interface{}) (result []map[string]string, err error) {
	return xmysql.Select(this.service, sql, args...)
}

func (this *MysqlDbClient) Find(output interface{}, sql string, args ...interface{}) error {
	return xmysql.Find(this.service, output, sql, args...)
}

func (this *MysqlDbClient) QueryWithCb(sqlFunc xmysql.RowScanCallback, sql string, args ...interface{}) (err error) {
	err = xmysql.QueryWithCb(sqlFunc, this.service, sql, args...)
	return
}

//TODO FindAll reflection to a obj array
